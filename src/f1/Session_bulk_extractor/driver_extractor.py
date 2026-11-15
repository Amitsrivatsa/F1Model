"""
This notebook is responsible for:
1. Requesting each year's race sessions.
2. Saving the original JSON response.
3. Uploading the file into the Unity Catalog Volume.

"""

import json
import logging
import hashlib
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from requests.exceptions import HTTPError

from f1.Session_bulk_extractor.manifest import ExtractionState, Manifest
from f1.Session_bulk_extractor.openf1_sessions import OpenF1Client
from pyspark.sql import SparkSession

LOG = logging.getLogger(__name__)

# --------------------------------------------------
# 6. Initialise Spark
# --------------------------------------------------


def get_spark():

    try:
        from databricks.connect import DatabricksSession

        return DatabricksSession.builder.serverless().getOrCreate()

    except ImportError:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()


spark = get_spark()

# --------------------------------------------------
# Configuration
# --------------------------------------------------

catalog = "f1_race_metrics"
schema = "raw"
volume = "landing"


start_year = 2023
endyear = 2025

sessions_basepath = f"/Volumes/{catalog}/{schema}/{volume}/openf1/sessions"
drivers_basepath = f"/Volumes/{catalog}/{schema}/{volume}/openf1/drivers"

w = WorkspaceClient()
manifest = Manifest(spark)

# --------------------------------------------------
# For each run generate the determistic requestid
# --------------------------------------------------


def generate_requestid(basepath, params):
    try:
        params = json.dumps(params, sort_keys=True, separators=(",", ":"))
        checksum = json.dumps({"endpoint": basepath, "params": params}, sort_keys=True, separators=(",", ":"))
        requestid = hashlib.sha256(checksum.encode("utf-8")).hexdigest()

        return requestid
    except Exception:
        LOG.exception("Unable to generate checksum")


# --------------------------------------------------
# checksum generation
# --------------------------------------------------


def sha256_file(path: Path):

    with path.open("rb") as src:
        return hashlib.file_digest(src, "sha256").hexdigest()


# --------------------------------------------------
# Verify Upload
# --------------------------------------------------
def verify_upload(
    w: WorkspaceClient,
    remote_path: str,
    local_path: Path,
    expected_sha256: str | None = None,
) -> None:
    """
    Check that a file was uploaded to a Unity Catalog volume correctly.

    Always checks that the file exists and that its size matches the local file.
    If expected_sha256 is given, also downloads the file and compares checksums.
    Raises RuntimeError if any check fails.
    """
    # 1. Exists
    try:
        meta = w.files.get_metadata(remote_path)
    except FileNotFoundError:
        raise RuntimeError(f"Upload failed, file not found: {remote_path}")

    # 2. Size matches
    local_size = local_path.stat().st_size
    if meta.content_length != local_size:
        raise RuntimeError(f"Size mismatch for {remote_path}: local={local_size}, remote={meta.content_length}")

    # 3. Content matches (optional)
    if expected_sha256 is not None:
        digest = hashlib.sha256()
        stream = w.files.download(remote_path).contents
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)

        if digest.hexdigest() != expected_sha256:
            raise RuntimeError(f"Checksum mismatch for {remote_path}")


# --------------------------------------------------
# Extract sessions
# --------------------------------------------------

with OpenF1Client() as api:
    for year in range(start_year, endyear + 1):
        # Read the sessions file already uploaded.
        sessions_directory = f"{sessions_basepath}/season={year}"

        files = [
            entry.path
            for entry in w.files.list_directory_contents(sessions_directory)
            if entry.path and entry.path.endswith(".json")
        ]

        if not files:
            raise RuntimeError(f"No sessions JSON found for {year}")

        # Your session filenames contain timestamps,
        # so the latest filename is the latest snapshot.
        latest_file = max(files)

        with w.files.download(latest_file).contents as stream:
            sessions = json.load(stream)

        for session in sessions:
            session_key = session["session_key"]

            params = {"session_key": session_key}

            requestid = generate_requestid("drivers", params)

            previous = manifest.read(requestid)

            if previous is not None and previous.status == "Complete":
                print(f"Skipping drivers: {session_key}")
                continue

            print(f"Extracting drivers: {session_key}")

            with tempfile.TemporaryDirectory() as temp_dir:
                local_path = Path(temp_dir) / "drivers.json"

                try:
                    api.download_json(endpoint="drivers", params=params, localfile=local_path)
                except HTTPError as error:
                    response = error.response
                    if response is not None and response.status_code == 404:
                        LOG.warning(
                            "Drivers unavailable for session_key=%s, year=%s",
                            session_key,
                            year,
                        )
                        continue
                    raise

                with local_path.open("r", encoding="utf-8") as file:
                    drivers = json.load(file)

                if not isinstance(drivers, list) or not drivers:
                    raise ValueError(f"Invalid drivers response: {session_key}")

                checksum = sha256_file(local_path)

                extracted_at = datetime.now(timezone.utc)

                directory = f"{drivers_basepath}/season={year}"

                filename = f"drivers_{session_key}_{extracted_at:%Y%m%dT%H%M%S%fZ}.json"

                destination = f"{directory}/{filename}"

                w.files.create_directory(directory)

                w.files.upload_from(destination, str(local_path), overwrite=False, use_parallel=False)

                verify_upload(w, destination, local_path, expected_sha256=checksum)

                state = ExtractionState(
                    request_id=requestid,
                    endpoint="drivers",
                    params_json=json.dumps(params, sort_keys=True, separators=(",", ":")),
                    status="Complete",
                    extracted_at=extracted_at,
                    file_checksum=checksum,
                )

                manifest.save(state)

                print(f"Saved {len(drivers)} drivers for session {session_key}")

print("\nDriver extraction completed.")
