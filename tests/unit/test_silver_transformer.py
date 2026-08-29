import pandas as pd

from nyc_taxi_etl.silver.transformer import SilverTransformer


def make_valid_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "bronze_trip_id": 1,
        "vendor_id": 2,
        "pickup_datetime": "2023-01-01 10:00:00",
        "dropoff_datetime": "2023-01-01 10:15:00",
        "passenger_count": 1,
        "trip_distance": 3.5,
        "rate_code_id": 1,
        "store_and_fwd_flag": "N",
        "pickup_location_id": 100,
        "dropoff_location_id": 200,
        "payment_type": 1,
        "fare_amount": 15.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 20.5,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
        "source_file": "yellow_tripdata_2023-01.parquet",
    }

    row.update(overrides)

    return row


def test_valid_trip_is_transformed() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(),
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert len(valid) == 1
    assert rejected.empty
    assert valid.iloc[0]["pickup_hour"] == 10
    assert valid.iloc[0]["trip_duration_minutes"] == 15.0


def test_negative_trip_distance_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=2,
                trip_distance=-1.0,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "invalid_trip_distance"


def test_negative_fare_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=3,
                fare_amount=-5.0,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "invalid_fare_amount"


def test_dropoff_before_pickup_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=4,
                pickup_datetime="2023-01-01 11:00:00",
                dropoff_datetime="2023-01-01 10:00:00",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "dropoff_before_pickup"


def test_missing_pickup_location_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=5,
                pickup_location_id=None,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "missing_pickup_location"


def test_missing_dropoff_location_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=6,
                dropoff_location_id=None,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "missing_dropoff_location"


def test_negative_total_amount_is_rejected() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=7,
                total_amount=-10.0,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "invalid_total_amount"


def test_mixed_batch_separates_valid_and_rejected_rows() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=10,
            ),
            make_valid_row(
                bronze_trip_id=11,
                fare_amount=-3.0,
            ),
            make_valid_row(
                bronze_trip_id=12,
                total_amount=-5.0,
            ),
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert len(valid) == 1
    assert len(rejected) == 2
    assert valid.iloc[0]["bronze_trip_id"] == 10


def test_batch_size_is_configurable() -> None:
    transformer = SilverTransformer(
        batch_size=10_000,
    )

    assert transformer.batch_size == 10_000


def test_rejects_trip_before_reporting_period() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=20,
                pickup_datetime="2022-12-31 23:50:00",
                dropoff_datetime="2022-12-31 23:59:00",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "outside_reporting_period"


def test_accepts_first_second_of_reporting_period() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=21,
                pickup_datetime="2023-01-01 00:00:00",
                dropoff_datetime="2023-01-01 00:10:00",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert len(valid) == 1
    assert rejected.empty


def test_accepts_trip_before_reporting_period_end() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=22,
                pickup_datetime="2023-02-28 23:40:00",
                dropoff_datetime="2023-02-28 23:50:00",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert len(valid) == 1
    assert rejected.empty


def test_rejects_trip_at_reporting_period_end() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=23,
                pickup_datetime="2023-03-01 00:00:00",
                dropoff_datetime="2023-03-01 00:10:00",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "outside_reporting_period"


def test_rejects_invalid_pickup_datetime() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=24,
                pickup_datetime="not-a-date",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "missing_or_invalid_pickup_datetime"


def test_rejects_invalid_dropoff_datetime() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=25,
                dropoff_datetime="not-a-date",
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1
    assert rejected.iloc[0]["rejection_reason"] == "missing_or_invalid_dropoff_datetime"


def test_rejection_rule_precedence_is_preserved() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(
                bronze_trip_id=26,
                pickup_datetime="2022-12-31 23:50:00",
                dropoff_datetime="2022-12-31 23:40:00",
                fare_amount=-5.0,
            )
        ]
    )

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert len(rejected) == 1

    # The first applicable rule wins.
    assert rejected.iloc[0]["rejection_reason"] == "dropoff_before_pickup"


def test_empty_dataframe_returns_empty_results() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame()

    valid, rejected = transformer.transform_batch(dataframe)

    assert valid.empty
    assert rejected.empty
    assert list(rejected.columns) == [
        "bronze_trip_id",
        "rejection_reason",
    ]
