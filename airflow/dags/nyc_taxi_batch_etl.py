from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pyarrow.parquet as pq
from airflow.sdk import dag, task

from nyc_taxi_etl.bronze.loader import BronzeLoader
from nyc_taxi_etl.gold.loader import GoldLoader
from nyc_taxi_etl.quality.checks import DataQualityChecker
from nyc_taxi_etl.silver.transformer import SilverTransformer

logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("/opt/airflow/data/raw")

SOURCE_FILES = (
    "yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet",
)


@dag(
    dag_id="nyc_taxi_batch_etl",
    description="NYC Yellow Taxi Jan-Feb 2023 medallion batch ETL",
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["nyc-taxi", "batch", "medallion"],
)
def nyc_taxi_batch_etl():

    @task
    def validate_source_files() -> None:
        logger.info("source_validation_started")

        total_rows = 0

        for filename in SOURCE_FILES:
            file_path = RAW_DATA_DIR / filename

            if not file_path.exists():
                raise FileNotFoundError(f"Required source file not found: {file_path}")

            parquet_file = pq.ParquetFile(file_path)
            row_count = parquet_file.metadata.num_rows

            if row_count <= 0:
                raise ValueError(f"Source file contains no rows: {file_path}")

            total_rows += row_count

            logger.info(
                "source_file_validated",
                extra={
                    "source_file": filename,
                    "source_row_count": row_count,
                },
            )

        logger.info(
            "source_validation_completed",
            extra={
                "file_count": len(SOURCE_FILES),
                "total_rows": total_rows,
            },
        )

    @task
    def load_bronze() -> None:
        logger.info("bronze_airflow_task_started")

        loader = BronzeLoader()

        total_loaded = 0

        for filename in SOURCE_FILES:
            file_path = RAW_DATA_DIR / filename
            loaded_rows = loader.load_file(file_path)
            total_loaded += loaded_rows

        logger.info(
            "bronze_airflow_task_completed",
            extra={
                "files_processed": len(SOURCE_FILES),
                "loaded_rows": total_loaded,
            },
        )

    @task
    def transform_silver() -> None:
        logger.info("silver_airflow_task_started")

        result = SilverTransformer().run()

        logger.info(
            "silver_airflow_task_completed",
            extra=result,
        )

    @task
    def load_gold() -> None:
        logger.info("gold_airflow_task_started")

        result = GoldLoader().run()

        logger.info(
            "gold_airflow_task_completed",
            extra=result,
        )

    @task
    def run_data_quality_checks() -> None:
        logger.info("data_quality_airflow_task_started")

        result = DataQualityChecker().run()

        logger.info(
            "data_quality_airflow_task_completed",
            extra={
                "checks_passed": len(result),
            },
        )

    @task
    def calculate_business_metrics() -> None:
        logger.info(
            "business_metrics_ready",
            extra={
                "sql_file": "/opt/airflow/sql/business_metrics.sql",
            },
        )

    validate_task = validate_source_files()
    bronze_task = load_bronze()
    silver_task = transform_silver()
    gold_task = load_gold()
    quality_task = run_data_quality_checks()
    metrics_task = calculate_business_metrics()

    (
        validate_task
        >> bronze_task
        >> silver_task
        >> gold_task
        >> quality_task
        >> metrics_task
    )


nyc_taxi_batch_etl()
