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

from f1.manifest import ExtractionState, Manifest
from f1.openf1_sessions import OpenF1Client
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
endpoint = "sessions"


start_year = 2023
endyear = 2025

basepath = f"/Volumes/{catalog}/{schema}/{volume}/openf1/sessions"

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
        params = {"year": year, "session_name": "Race"}

        # check if requestid is not populated for params
        requestid = generate_requestid(endpoint, params)

        if manifest.read(requestid) is None:
            print(f"\nExtracting race sessions: {year}")

            with tempfile.TemporaryDirectory() as temp_dir:
                local_path = Path(temp_dir) / "response.json"

                api.download_json(endpoint=endpoint, params=params, localfile=local_path)

                with local_path.open("r", encoding="utf-8") as file:
                    sessions = json.load(file)

                if not isinstance(sessions, list):
                    raise TypeError(f"Unexpected sessions response for {year}")

                # Extraction timestamp in UTC
                extracted_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

                # Organise by endpoint and season
                volume_path = f"{basepath}/season={year}"

                filename = f"sessions_{extracted_at}.json"

                destination = f"{volume_path}/{filename}"

                hashkey = sha256_file(local_path)

                # Create destination directory
                w.files.create_directory(volume_path)

                # Upload the original bytes
                w.files.upload_from(destination, str(local_path), overwrite=False, use_parallel=False)

                verify_upload(w, destination, local_path)

                state = ExtractionState(
                    request_id=requestid,
                    endpoint=endpoint,
                    params_json=json.dumps(params, sort_keys=True, separators=(",", ":")),
                    status="Complete",
                    extracted_at=datetime.now(timezone.utc),
                    file_checksum=hashkey,
                )

                manifest.save(state)

                print(f"Sessions found: {len(sessions)}")
                print(f"Saved: {destination}")

print("\nSession extraction completed.")
