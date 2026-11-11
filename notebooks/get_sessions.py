"""
This notebook is responsible for:
1. Requesting each year's race sessions.
2. Saving the original JSON response.
3. Uploading the file into the Unity Catalog Volume.

"""

import json
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from f1.openf1 import OpenF1Client


try:
    spark
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()

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


# --------------------------------------------------
# Create required Databricks objects
# --------------------------------------------------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

w = WorkspaceClient()

# --------------------------------------------------
# Extract sessions
# --------------------------------------------------

with OpenF1Client() as api:
    for year in range(start_year, endyear + 1):
        params = {"year": year, "session_name": "Race"}

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

            # Create destination directory
            w.files.create_directory(volume_path)

            # Upload the original bytes
            w.files.upload_from(destination, str(local_path), overwrite=False, use_parallel=False)

            print(f"Sessions found: {len(sessions)}")
            print(f"Saved: {destination}")

print("\nSession extraction completed.")
