from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F


# --------------------------------------------------
# Spark connection
# --------------------------------------------------

try:
    spark
except NameError:
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.serverless().getOrCreate()


# --------------------------------------------------
# Configuration
# --------------------------------------------------

CATALOG = "f1_race_metrics"

RAW_PATH = f"/Volumes/{CATALOG}/raw/landing/openf1/sessions"

TABLE_NAME = f"{CATALOG}.bronze.openf1_sessions"

START_YEAR = 2023
END_YEAR = datetime.now(timezone.utc).year

w = WorkspaceClient()
