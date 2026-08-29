from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

from nyc_taxi_etl.database import get_connection

logger = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    """Raised when a pipeline data-quality check fails."""


@dataclass(frozen=True)
class QualityCheck:
    """Definition of a single SQL data-quality check."""

    name: str
    query: str
    expected_value: int


QUALITY_CHECKS = (
    QualityCheck(
        name="bronze_has_rows",
        query="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1
                ELSE 0
            END
            FROM bronze.taxi_trips
        """,
        expected_value=1,
    ),
    QualityCheck(
        name="silver_has_rows",
        query="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1
                ELSE 0
            END
            FROM silver.taxi_trips
        """,
        expected_value=1,
    ),
    QualityCheck(
        name="gold_fact_has_rows",
        query="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1
                ELSE 0
            END
            FROM gold.fact_taxi_trips
        """,
        expected_value=1,
    ),
    QualityCheck(
        name="silver_no_null_pickup_datetime",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE pickup_datetime IS NULL
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_null_dropoff_datetime",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE dropoff_datetime IS NULL
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_valid_datetime_order",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE dropoff_datetime < pickup_datetime
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_negative_trip_distance",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE trip_distance < 0
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_negative_fare",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE fare_amount < 0
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_negative_total_amount",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE total_amount < 0
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_within_reporting_period",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE pickup_datetime < TIMESTAMP '2023-01-01 00:00:00'
               OR pickup_datetime >= TIMESTAMP '2023-03-01 00:00:00'
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="gold_no_duplicate_silver_trip_ids",
        query="""
            SELECT COUNT(*)
            FROM (
                SELECT silver_trip_id
                FROM gold.fact_taxi_trips
                GROUP BY silver_trip_id
                HAVING COUNT(*) > 1
            ) AS duplicates
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="gold_no_null_pickup_date_key",
        query="""
            SELECT COUNT(*)
            FROM gold.fact_taxi_trips
            WHERE pickup_date_key IS NULL
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_null_pickup_location",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE pickup_location_id IS NULL
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_no_null_dropoff_location",
        query="""
            SELECT COUNT(*)
            FROM silver.taxi_trips
            WHERE dropoff_location_id IS NULL
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="bronze_reconciles_with_silver_and_rejected",
        query="""
            SELECT ABS(
                (SELECT COUNT(*) FROM bronze.taxi_trips)
                -
                (
                    (SELECT COUNT(*) FROM silver.taxi_trips)
                    +
                    (SELECT COUNT(*) FROM silver.rejected_taxi_trips)
                )
            )
        """,
        expected_value=0,
    ),
    QualityCheck(
        name="silver_reconciles_with_gold_fact",
        query="""
            SELECT ABS(
                (SELECT COUNT(*) FROM silver.taxi_trips)
                -
                (SELECT COUNT(*) FROM gold.fact_taxi_trips)
            )
        """,
        expected_value=0,
    ),
)


class DataQualityChecker:
    """Run post-load data-quality checks against the warehouse."""

    def run(self) -> dict[str, int]:
        logger.info(
            "data_quality_checks_started",
            extra={
                "check_count": len(QUALITY_CHECKS),
            },
        )

        results: dict[str, int] = {}

        try:
            with get_connection() as connection:
                for check in QUALITY_CHECKS:
                    actual_value = self._execute_check(
                        connection,
                        check,
                    )

                    results[check.name] = actual_value

                    if actual_value != check.expected_value:
                        logger.error(
                            "data_quality_check_failed",
                            extra={
                                "check_name": check.name,
                                "expected_value": check.expected_value,
                                "actual_value": actual_value,
                            },
                        )

                        raise DataQualityError(
                            f"Data quality check failed: "
                            f"{check.name}. "
                            f"Expected {check.expected_value}, "
                            f"got {actual_value}."
                        )

                    logger.info(
                        "data_quality_check_passed",
                        extra={
                            "check_name": check.name,
                            "expected_value": check.expected_value,
                            "actual_value": actual_value,
                        },
                    )

        except Exception:
            logger.exception(
                "data_quality_checks_failed",
            )
            raise

        logger.info(
            "data_quality_checks_completed",
            extra={
                "checks_passed": len(results),
            },
        )

        return results

    @staticmethod
    def _execute_check(
        connection: Connection,
        check: QualityCheck,
    ) -> int:
        result = connection.execute(text(check.query)).scalar_one()

        return int(result)
