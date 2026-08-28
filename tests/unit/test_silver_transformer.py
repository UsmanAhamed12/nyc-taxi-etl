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

    dataframe = pd.DataFrame([make_valid_row()])

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

    assert rejected.iloc[0]["rejection_reason"] == "missing_pickup_location"


def test_mixed_batch_separates_valid_and_rejected_rows() -> None:
    transformer = SilverTransformer()

    dataframe = pd.DataFrame(
        [
            make_valid_row(bronze_trip_id=10),
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
    transformer = SilverTransformer(batch_size=10_000)

    assert transformer.batch_size == 10_000
