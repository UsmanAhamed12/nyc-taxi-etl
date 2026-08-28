#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${PROJECT_ROOT}/data/raw"

JAN_FILE="yellow_tripdata_2023-01.parquet"
FEB_FILE="yellow_tripdata_2023-02.parquet"

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"

echo "========================================"
echo " NYC Taxi ETL - Local Environment Setup "
echo "========================================"

cd "${PROJECT_ROOT}"

echo
echo "[1/5] Checking required tools..."

for command in uv docker curl; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "ERROR: '${command}' is required but was not found."
        exit 1
    fi
done

if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is installed but the Docker daemon is not running."
    echo "Start Docker Desktop and run ./setup.sh again."
    exit 1
fi

echo "Required tools are available."

echo
echo "[2/5] Creating Python 3.12 environment and installing dependencies..."

uv sync --python 3.12

echo "Python environment is ready."

echo
echo "[3/5] Preparing environment configuration..."

if [[ ! -f ".env" ]]; then
    cp .env.example .env
    echo "Created .env from .env.example."
else
    echo ".env already exists. Leaving it unchanged."
fi

echo
echo "[4/5] Starting PostgreSQL warehouse..."

docker compose up -d warehouse-db

echo "Waiting for PostgreSQL to become healthy..."

until docker exec nyc-taxi-warehouse \
    pg_isready \
    -U nyc_taxi \
    -d nyc_taxi \
    >/dev/null 2>&1; do
    sleep 2
done

echo "PostgreSQL warehouse is ready."

echo
echo "[5/5] Downloading NYC Yellow Taxi data..."

mkdir -p "${DATA_DIR}"

download_file() {
    local filename="$1"
    local url="${BASE_URL}/${filename}"
    local destination="${DATA_DIR}/${filename}"

    if [[ -f "${destination}" ]]; then
        echo "${filename} already exists. Skipping download."
        return
    fi

    echo "Downloading ${filename}..."

    curl \
        --fail \
        --location \
        --retry 3 \
        --retry-delay 2 \
        --output "${destination}" \
        "${url}"

    echo "Downloaded ${filename}."
}

download_file "${JAN_FILE}"
download_file "${FEB_FILE}"

echo
echo "========================================"
echo " Setup completed successfully "
echo "========================================"
echo
echo "Downloaded files:"
ls -lh "${DATA_DIR}"/*.parquet