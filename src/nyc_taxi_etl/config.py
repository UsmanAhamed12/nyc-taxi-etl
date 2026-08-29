from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    postgres_host: str = "localhost"
    postgres_port: int = 5434
    postgres_db: str = "nyc_taxi"
    postgres_user: str = "nyc_taxi"
    postgres_password: str = "nyc_taxi_password"

    environment: str = "local"
    tlc_data_dir: Path = Path("data/raw")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://"
            f"{self.postgres_user}:"
            f"{self.postgres_password}@"
            f"{self.postgres_host}:"
            f"{self.postgres_port}/"
            f"{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Return one cached Settings instance per Python process."""
    return Settings()
