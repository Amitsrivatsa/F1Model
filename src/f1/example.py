"""Checkpointed OpenF1 sessions extractor (single writer)."""

import argparse
import hashlib
import json
import logging
import tempfile

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from f1.manifest import ExtractionState, Manifest
from f1.openf1 import OpenF1Client


LOG = logging.getLogger(__name__)


# --------------------------------------------------
# 1. Deterministic request ID
# --------------------------------------------------


def request_identity(endpoint: str, params: dict) -> tuple[str, str]:

    params_json = json.dumps(params, sort_keys=True, separators=(",", ":"))

    identity = json.dumps({"endpoint": endpoint, "params": params}, sort_keys=True, separators=(",", ":"))

    request_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()

    return request_id, params_json


# --------------------------------------------------
# 2. Calculate local file checksum
# --------------------------------------------------


def sha256_file(path: Path) -> str:

    digest = hashlib.sha256()

    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


# --------------------------------------------------
# 3. Check whether remote file exists
# --------------------------------------------------


def remote_exists(workspace, path: str) -> bool:

    from databricks.sdk.errors import NotFound

    try:
        workspace.files.get_metadata(path)
        return True

    except NotFound:
        return False


# --------------------------------------------------
# 4. Verify uploaded file
# --------------------------------------------------


def verify_remote(workspace, path: str, expected_sha: str, expected_bytes: int) -> bool:

    if not remote_exists(workspace, path):
        return False

    with tempfile.TemporaryDirectory() as folder:
        copy = Path(folder) / "verify.json"

        workspace.files.download_to(path, str(copy), use_parallel=False)

        if copy.stat().st_size != expected_bytes or sha256_file(copy) != expected_sha:
            raise RuntimeError(f"Remote checksum mismatch: {path}")

    return True


# --------------------------------------------------
# 5. Extract one season
# --------------------------------------------------


def extract_one(year: int, mode: str, catalog: str, manifest, workspace, api) -> str:

    if year < 2023:
        raise ValueError("OpenF1 historical data starts in 2023")

    if mode not in ("normal", "refresh"):
        raise ValueError("Mode must be normal or refresh")

    endpoint = "sessions"

    params = {"year": year, "session_name": "Race"}

    request_id, params_json = request_identity(endpoint, params)

    # Retrieve previous extraction state.
    state = manifest.read(request_id) or ExtractionState(
        request_id=request_id, endpoint=endpoint, season=year, params_json=params_json
    )

    # ------------------------------------------------
    # STEP 1: Recover interrupted upload
    # ------------------------------------------------

    if state.status == "PUBLISHING":
        if not (
            state.planned_path
            and state.planned_sha256
            and state.planned_bytes is not None
            and state.planned_extracted_at
        ):
            raise RuntimeError("Incomplete PUBLISHING state: manual inspection required")

        recovered = verify_remote(workspace, state.planned_path, state.planned_sha256, state.planned_bytes)

        if recovered:
            completed = replace(
                state,
                status="COMPLETED",
                raw_path=state.planned_path,
                sha256=state.planned_sha256,
                byte_size=state.planned_bytes,
                extracted_at=state.planned_extracted_at,
                planned_path=None,
                planned_sha256=None,
                planned_bytes=None,
                planned_extracted_at=None,
                last_error=None,
            )

            manifest.save(completed)

            print(f"RECOVERED {year}: {completed.raw_path}")

            return "RECOVERED"

        # Upload did not occur.
        # Clear planned state and retry extraction.
        state = replace(state, planned_path=None, planned_sha256=None, planned_bytes=None, planned_extracted_at=None)

    # ------------------------------------------------
    # STEP 2: Skip previously completed work
    # ------------------------------------------------

    if mode == "normal" and state.raw_path and state.sha256:
        if not remote_exists(workspace, state.raw_path):
            raise RuntimeError(f"Manifest refers to missing raw file: {state.raw_path}")

        if state.status != "COMPLETED":
            manifest.save(replace(state, status="COMPLETED", last_error=None))

        print(f"SKIPPED {year}: already extracted")

        return "SKIPPED"

    # ------------------------------------------------
    # STEP 3: Mark extraction as RUNNING
    # ------------------------------------------------

    state = replace(state, status="RUNNING", last_error=None)

    manifest.save(state)

    try:
        with tempfile.TemporaryDirectory() as folder:
            local = Path(folder) / "response.json"

            # ----------------------------------------
            # STEP 4: Download from OpenF1
            # ----------------------------------------

            api.download("sessions", params, local)

            # Sessions are small enough to validate.
            # Do not use this for large telemetry.
            with local.open(encoding="utf-8") as src:
                sessions = json.load(src)

            if (
                not isinstance(sessions, list)
                or not sessions
                or any(not isinstance(s, dict) or "session_key" not in s for s in sessions)
            ):
                raise ValueError(f"Invalid/empty sessions response for {year}")

            # ----------------------------------------
            # STEP 5: Calculate checksum
            # ----------------------------------------

            checksum = sha256_file(local)
            size = local.stat().st_size

            # ----------------------------------------
            # STEP 6: Skip unchanged refresh
            # ----------------------------------------

            if checksum == state.sha256 and state.raw_path:
                if not remote_exists(workspace, state.raw_path):
                    raise RuntimeError(f"Missing prior raw file: {state.raw_path}")

                manifest.save(replace(state, status="COMPLETED", last_error=None))

                print(f"UNCHANGED {year}: same checksum")

                return "UNCHANGED"

            # ----------------------------------------
            # STEP 7: Construct destination path
            # ----------------------------------------

            now = datetime.now(timezone.utc)

            stamp = now.strftime("%Y%m%dT%H%M%S%fZ")

            extracted_at = now.strftime("%Y-%m-%d %H:%M:%S.%f")

            directory = f"/Volumes/{catalog}/raw/landing/openf1/sessions/season={year}"

            destination = f"{directory}/sessions_{request_id[:12]}_{stamp}_{checksum}.json"

            # ----------------------------------------
            # STEP 8: Record intended upload
            # ----------------------------------------

            state = replace(
                state,
                status="PUBLISHING",
                planned_path=destination,
                planned_sha256=checksum,
                planned_bytes=size,
                planned_extracted_at=extracted_at,
            )

            # Save BEFORE attempting upload.
            manifest.save(state)

            # ----------------------------------------
            # STEP 9: Upload original JSON
            # ----------------------------------------

            workspace.files.create_directory(directory)

            workspace.files.upload_from(destination, str(local), overwrite=False, use_parallel=False)

            # ----------------------------------------
            # STEP 10: Verify uploaded file
            # ----------------------------------------

            if not verify_remote(workspace, destination, checksum, size):
                raise RuntimeError(f"Upload missing: {destination}")

            # ----------------------------------------
            # STEP 11: Mark COMPLETED
            # ----------------------------------------

            completed = replace(
                state,
                status="COMPLETED",
                raw_path=destination,
                sha256=checksum,
                byte_size=size,
                extracted_at=extracted_at,
                planned_path=None,
                planned_sha256=None,
                planned_bytes=None,
                planned_extracted_at=None,
                last_error=None,
            )

            manifest.save(completed)

            print(f"PUBLISHED {year}: {len(sessions)} sessions -> {destination}")

            return "PUBLISHED"

    except Exception as exc:
        # Preserve previous successful publication
        # and any planned upload for recovery.
        failed = replace(state, status=("PUBLISHING" if state.planned_path else "FAILED"), last_error=str(exc)[:1000])

        try:
            manifest.save(failed)

        except Exception:
            LOG.exception("Unable to save extraction failure")

        raise


# --------------------------------------------------
# 6. Initialise Spark
# --------------------------------------------------


def get_spark():

    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.getOrCreate()

    except ImportError:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


# --------------------------------------------------
# 7. Application entry point
# --------------------------------------------------


def main(argv=None):

    parser = argparse.ArgumentParser(description="Extract OpenF1 race sessions")

    parser.add_argument("--start-year", type=int, default=2023)

    parser.add_argument("--end-year", type=int, default=datetime.now(timezone.utc).year)

    parser.add_argument("--mode", choices=["auto", "normal", "refresh"], default="auto")

    parser.add_argument("--catalog", default="f1_dev")

    args = parser.parse_args(argv)

    if args.start_year < 2023 or args.end_year < args.start_year:
        parser.error("Expected 2023 <= start-year <= end-year")

    from databricks.sdk import WorkspaceClient

    spark = get_spark()

    manifest = Manifest(spark)

    workspace = WorkspaceClient()

    with OpenF1Client() as api:
        current_year = datetime.now(timezone.utc).year

        for year in range(args.start_year, args.end_year + 1):
            mode = args.mode

            if mode == "auto":
                mode = "refresh" if year == current_year else "normal"

            extract_one(year, mode, args.catalog, manifest, workspace, api)


if __name__ == "__main__":
    main()
