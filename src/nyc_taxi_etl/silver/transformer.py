from __future__ import annotations

import logging
from time import perf_counter

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection

from nyc_taxi_etl.database import get_connection

logger = logging.getLogger(__name__)


SILVER_COLUMNS = [
    "bronze_trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "pickup_date",
    "pickup_hour",
    "trip_duration_minutes",
    "source_file",
]


class SilverTransformer:
    """Clean Bronze taxi trips and load valid records into Silver."""

    def __init__(self, batch_size: int = 50_000) -> None:
        self.batch_size = batch_size

    def transform_batch(
        self,
        dataframe: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return valid Silver rows and rejected Bronze rows."""

        if dataframe.empty:
            return dataframe.copy(), pd.DataFrame(
                columns=["bronze_trip_id", "rejection_reason"]
            )

        transformed = dataframe.copy()

        transformed["pickup_datetime"] = pd.to_datetime(
            transformed["pickup_datetime"],
            errors="coerce",
        )
        transformed["dropoff_datetime"] = pd.to_datetime(
            transformed["dropoff_datetime"],
            errors="coerce",
        )

        rejection_reason = pd.Series(
            pd.NA,
            index=transformed.index,
            dtype="string",
        )

        rejection_reason = rejection_reason.mask(
            transformed["pickup_datetime"].isna(),
            "missing_or_invalid_pickup_datetime",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna() & transformed["dropoff_datetime"].isna(),
            "missing_or_invalid_dropoff_datetime",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna()
            & transformed["pickup_datetime"].notna()
            & transformed["dropoff_datetime"].notna()
            & (transformed["dropoff_datetime"] < transformed["pickup_datetime"]),
            "dropoff_before_pickup",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna()
            & (
                transformed["trip_distance"].isna() | (transformed["trip_distance"] < 0)
            ),
            "invalid_trip_distance",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna()
            & (transformed["fare_amount"].isna() | (transformed["fare_amount"] < 0)),
            "invalid_fare_amount",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna()
            & (transformed["total_amount"].isna() | (transformed["total_amount"] < 0)),
            "invalid_total_amount",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna() & transformed["pickup_location_id"].isna(),
            "missing_pickup_location",
        )

        rejection_reason = rejection_reason.mask(
            rejection_reason.isna() & transformed["dropoff_location_id"].isna(),
            "missing_dropoff_location",
        )

        valid_mask = rejection_reason.isna()

        valid = transformed.loc[valid_mask].copy()

        valid["pickup_date"] = valid["pickup_datetime"].dt.date
        valid["pickup_hour"] = valid["pickup_datetime"].dt.hour

        valid["trip_duration_minutes"] = (
            valid["dropoff_datetime"] - valid["pickup_datetime"]
        ).dt.total_seconds() / 60

        valid = valid[SILVER_COLUMNS]

        rejected = transformed.loc[
            ~valid_mask,
            ["bronze_trip_id"],
        ].copy()

        rejected["rejection_reason"] = rejection_reason.loc[~valid_mask].astype(str)

        return valid, rejected

    def run(
        self,
        max_batches: int | None = None,
    ) -> dict[str, int | float]:
        start_time = perf_counter()

        total_read = 0
        total_loaded = 0
        total_rejected = 0
        batch_count = 0

        logger.info(
            "silver_transform_started",
            extra={"batch_size": self.batch_size},
        )

        while True:
            with get_connection() as connection:
                dataframe = self._read_next_batch(connection)

                if dataframe.empty:
                    break

                valid, rejected = self.transform_batch(dataframe)

                if not valid.empty:
                    self._load_valid_batch(connection, valid)

                if not rejected.empty:
                    self._load_rejected_batch(connection, rejected)

                rows_read = len(dataframe)
                rows_loaded = len(valid)
                rows_rejected = len(rejected)

                total_read += rows_read
                total_loaded += rows_loaded
                total_rejected += rows_rejected
                batch_count += 1

                logger.info(
                    "silver_batch_completed",
                    extra={
                        "batch_number": batch_count,
                        "rows_read": rows_read,
                        "rows_loaded": rows_loaded,
                        "rows_rejected": rows_rejected,
                    },
                )

            if max_batches is not None and batch_count >= max_batches:
                break

        duration_seconds = round(
            perf_counter() - start_time,
            2,
        )

        logger.info(
            "silver_transform_completed",
            extra={
                "batches_processed": batch_count,
                "rows_read": total_read,
                "rows_loaded": total_loaded,
                "rows_rejected": total_rejected,
                "duration_seconds": duration_seconds,
            },
        )

        return {
            "batches_processed": batch_count,
            "rows_read": total_read,
            "rows_loaded": total_loaded,
            "rows_rejected": total_rejected,
            "duration_seconds": duration_seconds,
        }

    def _read_next_batch(
        self,
        connection: Connection,
    ) -> pd.DataFrame:
        query = text(
            """
            SELECT
                b.bronze_trip_id,
                b.vendor_id,
                b.pickup_datetime,
                b.dropoff_datetime,
                b.passenger_count,
                b.trip_distance,
                b.rate_code_id,
                b.store_and_fwd_flag,
                b.pickup_location_id,
                b.dropoff_location_id,
                b.payment_type,
                b.fare_amount,
                b.extra,
                b.mta_tax,
                b.tip_amount,
                b.tolls_amount,
                b.improvement_surcharge,
                b.total_amount,
                b.congestion_surcharge,
                b.airport_fee,
                b.source_file
            FROM bronze.taxi_trips AS b
            WHERE NOT EXISTS (
                SELECT 1
                FROM silver.taxi_trips AS s
                WHERE s.bronze_trip_id = b.bronze_trip_id
            )
            AND NOT EXISTS (
                SELECT 1
                FROM silver.rejected_taxi_trips AS r
                WHERE r.bronze_trip_id = b.bronze_trip_id
            )
            ORDER BY b.bronze_trip_id
            LIMIT :batch_size
            """
        )

        return pd.read_sql(
            query,
            connection,
            params={"batch_size": self.batch_size},
        )

    @staticmethod
    def _load_valid_batch(
        connection: Connection,
        dataframe: pd.DataFrame,
    ) -> None:
        dataframe.to_sql(
            name="taxi_trips",
            con=connection,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000,
        )

    @staticmethod
    def _load_rejected_batch(
        connection: Connection,
        dataframe: pd.DataFrame,
    ) -> None:
        dataframe.to_sql(
            name="rejected_taxi_trips",
            con=connection,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5_000,
        )
