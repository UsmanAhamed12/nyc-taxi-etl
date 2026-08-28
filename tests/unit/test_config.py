from nyc_taxi_etl.config import Settings


def test_database_url() -> None:
    settings = Settings(
        postgres_host="db",
        postgres_port=5432,
        postgres_db="warehouse",
        postgres_user="user",
        postgres_password="password",
    )

    assert (
        settings.database_url == "postgresql+psycopg2://user:password@db:5432/warehouse"
    )
