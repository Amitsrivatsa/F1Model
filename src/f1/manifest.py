"""
This module manages our extraction state in Delta.

The important feature is that we preserve the last successful file even if a later refresh fails.
"""

from dataclasses import asdict, dataclass


@dataclass
class ExtractionState:
    request_id: str
    endpoint: str
    params_json: str
    status: str = "NEW"
    file_checksum: str | None = None
    extracted_at: str | None = None


class Manifest:
    def __init__(self, spark):

        self.spark = spark

        self.table = "f1_race_metrics.ops.extraction_manifest"

        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

    def read(self, request_id: str) -> ExtractionState | None:

        rows = self.spark.sql(
            f"""
            SELECT
                request_id,
                endpoint,
                params_json,
                status,
                file_checksum,
                CAST(extracted_at AS STRING) AS extracted_at
            FROM {self.table}
            WHERE request_id = :request_id
            LIMIT 2
            """,
            args={"request_id": request_id},
        ).collect()

        if len(rows) > 1:
            raise RuntimeError(f"Duplicate manifest request_id: {request_id}")

        return ExtractionState(**rows[0].asDict()) if rows else None

    def save(self, state: ExtractionState) -> None:
        """Upsert request state into Delta."""

        self.spark.sql(
            f"""
            MERGE INTO {self.table} AS target

            USING (
                SELECT
                    :request_id AS request_id,
                    :endpoint AS endpoint,
                    :params_json AS params_json,
                    :status AS status,
                    CAST(:file_checksum AS STRING) AS file_checksum,
                    CAST(:extracted_at AS TIMESTAMP) AS extracted_at
            ) AS source

            ON target.request_id = source.request_id

            WHEN MATCHED THEN UPDATE SET *

            WHEN NOT MATCHED THEN INSERT *
            """,
            args=asdict(state),
        ).collect()
