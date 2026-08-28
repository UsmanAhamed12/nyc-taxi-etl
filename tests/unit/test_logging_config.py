import json
import logging

from nyc_taxi_etl.logging_config import JsonFormatter


def test_json_formatter_includes_extra_fields() -> None:
    formatter = JsonFormatter()

    record = logging.LogRecord(
        name="nyc_taxi_etl",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="test_event",
        args=(),
        exc_info=None,
    )

    record.rows_processed = 123

    formatted = formatter.format(record)
    payload = json.loads(formatted)

    assert payload["level"] == "INFO"
    assert payload["message"] == "test_event"
    assert payload["rows_processed"] == 123
