from contextlib import contextmanager
from unittest.mock import patch

import pytest

from nyc_taxi_etl.quality.checks import (
    DataQualityChecker,
    DataQualityError,
    QualityCheck,
)


class FakeResult:
    def __init__(self, value: int) -> None:
        self.value = value

    def scalar_one(self) -> int:
        return self.value


class FakeConnection:
    def __init__(self, value: int) -> None:
        self.value = value

    def execute(self, query: object) -> FakeResult:
        return FakeResult(self.value)


def test_quality_check_returns_actual_value() -> None:
    checker = DataQualityChecker()

    check = QualityCheck(
        name="test_check",
        query="SELECT 0",
        expected_value=0,
    )

    connection = FakeConnection(0)

    result = checker._execute_check(
        connection,  # type: ignore[arg-type]
        check,
    )

    assert result == 0


def test_quality_checker_raises_when_check_fails() -> None:
    checker = DataQualityChecker()

    failing_check = QualityCheck(
        name="no_bad_rows",
        query="SELECT COUNT(*) FROM bad_rows",
        expected_value=0,
    )

    @contextmanager
    def fake_connection():
        yield FakeConnection(5)

    with (
        patch(
            "nyc_taxi_etl.quality.checks.QUALITY_CHECKS",
            (failing_check,),
        ),
        patch(
            "nyc_taxi_etl.quality.checks.get_connection",
            fake_connection,
        ),
        pytest.raises(
            DataQualityError,
            match="no_bad_rows",
        ),
    ):
        checker.run()
