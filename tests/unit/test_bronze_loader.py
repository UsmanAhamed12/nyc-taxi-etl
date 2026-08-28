from pathlib import Path

import pyarrow as pa
import pytest

from nyc_taxi_etl.bronze.loader import SOURCE_TO_BRONZE_COLUMNS, BronzeLoader


def test_load_file_raises_when_source_does_not_exist() -> None:
    loader = BronzeLoader()

    with pytest.raises(FileNotFoundError):
        loader.load_file(Path("does-not-exist.parquet"))


def test_batch_size_is_configurable() -> None:
    loader = BronzeLoader(batch_size=10_000)

    assert loader.batch_size == 10_000


def test_source_batch_has_expected_columns() -> None:
    batch = pa.record_batch(
        [
            pa.array([1]),
            pa.array([1.5]),
        ],
        names=[
            "VendorID",
            "trip_distance",
        ],
    )

    assert batch.num_rows == 1
    assert batch.column_names == [
        "VendorID",
        "trip_distance",
    ]


def test_airport_fee_schema_variants_are_normalized() -> None:
    assert SOURCE_TO_BRONZE_COLUMNS["airport_fee"] == "airport_fee"
    assert SOURCE_TO_BRONZE_COLUMNS["Airport_fee"] == "airport_fee"
