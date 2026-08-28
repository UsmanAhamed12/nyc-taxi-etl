import logging
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.engine import Connection

from nyc_taxi_etl.database import get_connection

logger = logging.getLogger(__name__)


SOURCE_TO_BRONZE_COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}


class BronzeLoader:
    """Load NYC Yellow Taxi Parquet files into the Bronze layer."""

    def __init__(self, batch_size: int = 50_000) -> None:
        self.batch_size = batch_size

    def load_file(self, path: Path) -> int:
        """Load one Parquet file and return the number of inserted rows."""
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {path}")

        parquet_file = pq.ParquetFile(path)
        source_row_count = parquet_file.metadata.num_rows

        logger.info(
            "bronze_load_started",
            extra={
                "source_file": path.name,
                "source_row_count": source_row_count,
            },
        )

        start_time = perf_counter()

        try:
            with get_connection() as connection:
                if self._is_completed(connection, path.name):
                    logger.info(
                        "bronze_load_skipped",
                        extra={
                            "source_file": path.name,
                            "reason": "already_completed",
                        },
                    )
                    return 0

                self._mark_started(
                    connection=connection,
                    source_file=path.name,
                    source_row_count=source_row_count,
                )

                loaded_row_count = 0

                for batch in parquet_file.iter_batches(
                    batch_size=self.batch_size,
                ):
                    loaded_row_count += self._load_batch(
                        connection=connection,
                        batch=batch,
                        source_file=path.name,
                    )

                if loaded_row_count != source_row_count:
                    raise ValueError(
                        "Bronze row-count validation failed for "
                        f"{path.name}: source={source_row_count}, "
                        f"loaded={loaded_row_count}"
                    )

                self._mark_completed(
                    connection=connection,
                    source_file=path.name,
                    loaded_row_count=loaded_row_count,
                )

            duration_seconds = round(perf_counter() - start_time, 2)

            logger.info(
                "bronze_load_completed",
                extra={
                    "source_file": path.name,
                    "rows_processed": loaded_row_count,
                    "duration_seconds": duration_seconds,
                },
            )

            return loaded_row_count

        except (OSError, ValueError):
            logger.exception(
                "bronze_load_failed",
                extra={"source_file": path.name},
            )
            raise

        except Exception:
            logger.exception(
                "bronze_load_failed_unexpected",
                extra={"source_file": path.name},
            )
            raise

    def _load_batch(
        self,
        connection: Connection,
        batch: pa.RecordBatch,
        source_file: str,
    ) -> int:
        """Insert one Arrow batch into bronze.taxi_trips."""
        dataframe = batch.to_pandas()

        dataframe = dataframe.rename(columns=SOURCE_TO_BRONZE_COLUMNS)

        dataframe["source_file"] = source_file

        dataframe.to_sql(
            name="taxi_trips",
            con=connection,
            schema="bronze",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000,
        )

        return len(dataframe)

    @staticmethod
    def _is_completed(
        connection: Connection,
        source_file: str,
    ) -> bool:
        result = connection.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM bronze.ingestion_audit
                    WHERE source_file = :source_file
                      AND status = 'COMPLETED'
                )
                """
            ),
            {"source_file": source_file},
        )

        return bool(result.scalar_one())

    @staticmethod
    def _mark_started(
        connection: Connection,
        source_file: str,
        source_row_count: int,
    ) -> None:
        connection.execute(
            text(
                """
                INSERT INTO bronze.ingestion_audit (
                    source_file,
                    source_row_count,
                    loaded_row_count,
                    status,
                    started_at,
                    completed_at
                )
                VALUES (
                    :source_file,
                    :source_row_count,
                    0,
                    'STARTED',
                    CURRENT_TIMESTAMP,
                    NULL
                )
                ON CONFLICT (source_file)
                DO UPDATE SET
                    source_row_count = EXCLUDED.source_row_count,
                    loaded_row_count = 0,
                    status = 'STARTED',
                    started_at = CURRENT_TIMESTAMP,
                    completed_at = NULL
                """
            ),
            {
                "source_file": source_file,
                "source_row_count": source_row_count,
            },
        )

    @staticmethod
    def _mark_completed(
        connection: Connection,
        source_file: str,
        loaded_row_count: int,
    ) -> None:
        connection.execute(
            text(
                """
                UPDATE bronze.ingestion_audit
                SET loaded_row_count = :loaded_row_count,
                    status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP
                WHERE source_file = :source_file
                """
            ),
            {
                "source_file": source_file,
                "loaded_row_count": loaded_row_count,
            },
        )
