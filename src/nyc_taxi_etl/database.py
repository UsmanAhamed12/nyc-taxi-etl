from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from nyc_taxi_etl.config import get_settings

settings = get_settings()


engine: Engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)


@contextmanager
def get_connection() -> Generator[Connection, None, None]:
    """
    Provide a transactional database connection.

    The transaction is committed automatically when the block succeeds
    and rolled back automatically if an exception is raised.
    """
    with engine.begin() as connection:
        yield connection


def check_database_connection() -> bool:
    """Return True when the PostgreSQL warehouse is reachable."""
    try:
        with get_connection() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError:
        return False

    return True
