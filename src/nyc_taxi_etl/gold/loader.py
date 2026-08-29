import logging
from time import perf_counter

from sqlalchemy import text
from sqlalchemy.engine import Connection

from nyc_taxi_etl.database import get_connection

logger = logging.getLogger(__name__)


class GoldLoader:
    """Load dimensional models from cleaned Silver taxi trips."""

    def run(self) -> dict[str, int | float]:
        start_time = perf_counter()

        logger.info("gold_load_started")

        try:
            with get_connection() as connection:
                dates_loaded = self._load_date_dimension(connection)
                locations_loaded = self._load_location_dimension(connection)
                payment_types_loaded = self._load_payment_type_dimension(connection)
                vendors_loaded = self._load_vendor_dimension(connection)
                facts_loaded = self._load_fact_table(connection)

            duration_seconds = round(
                perf_counter() - start_time,
                2,
            )

            result = {
                "dates_loaded": dates_loaded,
                "locations_loaded": locations_loaded,
                "payment_types_loaded": payment_types_loaded,
                "vendors_loaded": vendors_loaded,
                "facts_loaded": facts_loaded,
                "duration_seconds": duration_seconds,
            }

            logger.info(
                "gold_load_completed",
                extra=result,
            )

            return result

        except Exception:
            logger.exception("gold_load_failed")
            raise

    @staticmethod
    def _load_date_dimension(connection: Connection) -> int:
        result = connection.execute(
            text(
                """
                INSERT INTO gold.dim_date (
                    date_key,
                    full_date,
                    year,
                    quarter,
                    month,
                    month_name,
                    day,
                    day_of_week,
                    day_name,
                    is_weekend
                )
                SELECT DISTINCT
                    TO_CHAR(pickup_date, 'YYYYMMDD')::INTEGER,
                    pickup_date,
                    EXTRACT(YEAR FROM pickup_date)::SMALLINT,
                    EXTRACT(QUARTER FROM pickup_date)::SMALLINT,
                    EXTRACT(MONTH FROM pickup_date)::SMALLINT,
                    TO_CHAR(pickup_date, 'FMMonth'),
                    EXTRACT(DAY FROM pickup_date)::SMALLINT,
                    EXTRACT(ISODOW FROM pickup_date)::SMALLINT,
                    TO_CHAR(pickup_date, 'FMDay'),
                    EXTRACT(ISODOW FROM pickup_date) IN (6, 7)
                FROM silver.taxi_trips
                WHERE pickup_date IS NOT NULL
                ON CONFLICT (date_key) DO NOTHING
                """
            )
        )

        return result.rowcount

    @staticmethod
    def _load_location_dimension(connection: Connection) -> int:
        result = connection.execute(
            text(
                """
                INSERT INTO gold.dim_location (
                    source_location_id
                )
                SELECT location_id
                FROM (
                    SELECT pickup_location_id AS location_id
                    FROM silver.taxi_trips

                    UNION

                    SELECT dropoff_location_id AS location_id
                    FROM silver.taxi_trips
                ) AS locations
                WHERE location_id IS NOT NULL
                ON CONFLICT (source_location_id) DO NOTHING
                """
            )
        )

        return result.rowcount

    @staticmethod
    def _load_payment_type_dimension(
        connection: Connection,
    ) -> int:
        result = connection.execute(
            text(
                """
                INSERT INTO gold.dim_payment_type (
                    payment_type_code,
                    payment_type_name
                )
                SELECT DISTINCT
                    payment_type,
                    CASE payment_type
                        WHEN 1 THEN 'Credit card'
                        WHEN 2 THEN 'Cash'
                        WHEN 3 THEN 'No charge'
                        WHEN 4 THEN 'Dispute'
                        WHEN 5 THEN 'Unknown'
                        WHEN 6 THEN 'Voided trip'
                        ELSE 'Unknown'
                    END
                FROM silver.taxi_trips
                WHERE payment_type IS NOT NULL
                ON CONFLICT (payment_type_code) DO NOTHING
                """
            )
        )

        return result.rowcount

    @staticmethod
    def _load_vendor_dimension(connection: Connection) -> int:
        result = connection.execute(
            text(
                """
                INSERT INTO gold.dim_vendor (
                    vendor_id,
                    vendor_name
                )
                SELECT DISTINCT
                    vendor_id,
                    CASE vendor_id
                        WHEN 1 THEN 'Creative Mobile Technologies'
                        WHEN 2 THEN 'VeriFone'
                        ELSE 'Unknown'
                    END
                FROM silver.taxi_trips
                WHERE vendor_id IS NOT NULL
                ON CONFLICT (vendor_id) DO NOTHING
                """
            )
        )

        return result.rowcount

    @staticmethod
    def _load_fact_table(connection: Connection) -> int:
        result = connection.execute(
            text(
                """
                INSERT INTO gold.fact_taxi_trips (
                    silver_trip_id,
                    pickup_date_key,
                    pickup_location_key,
                    dropoff_location_key,
                    payment_type_key,
                    vendor_key,
                    pickup_datetime,
                    dropoff_datetime,
                    pickup_hour,
                    passenger_count,
                    trip_distance,
                    trip_duration_minutes,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    congestion_surcharge,
                    airport_fee,
                    total_amount
                )
                SELECT
                    s.silver_trip_id,
                    d.date_key,
                    pickup_location.location_key,
                    dropoff_location.location_key,
                    payment.payment_type_key,
                    vendor.vendor_key,
                    s.pickup_datetime,
                    s.dropoff_datetime,
                    s.pickup_hour,
                    s.passenger_count,
                    s.trip_distance,
                    s.trip_duration_minutes,
                    s.fare_amount,
                    s.extra,
                    s.mta_tax,
                    s.tip_amount,
                    s.tolls_amount,
                    s.improvement_surcharge,
                    s.congestion_surcharge,
                    s.airport_fee,
                    s.total_amount
                FROM silver.taxi_trips AS s

                INNER JOIN gold.dim_date AS d
                    ON d.full_date = s.pickup_date

                LEFT JOIN gold.dim_location AS pickup_location
                    ON pickup_location.source_location_id =
                       s.pickup_location_id

                LEFT JOIN gold.dim_location AS dropoff_location
                    ON dropoff_location.source_location_id =
                       s.dropoff_location_id

                LEFT JOIN gold.dim_payment_type AS payment
                    ON payment.payment_type_code =
                       s.payment_type

                LEFT JOIN gold.dim_vendor AS vendor
                    ON vendor.vendor_id =
                       s.vendor_id

                ON CONFLICT (silver_trip_id) DO NOTHING
                """
            )
        )

        return result.rowcount
