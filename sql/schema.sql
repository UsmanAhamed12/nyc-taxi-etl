-- ============================================================
-- NYC Yellow Taxi Batch ETL
-- Database schemas and dimensional model
--
-- Gold fact grain:
-- One row in gold.fact_taxi_trips represents one taxi trip.
-- ============================================================


-- ============================================================
-- 1. SCHEMAS
-- ============================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ============================================================
-- 2. BRONZE LAYER
-- ============================================================
-- Source-aligned representation of NYC Yellow Taxi trip data.
-- Only ingestion metadata is added.
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.taxi_trips (
    bronze_trip_id BIGSERIAL PRIMARY KEY,

    vendor_id BIGINT,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    rate_code_id DOUBLE PRECISION,
    store_and_fwd_flag TEXT,
    pickup_location_id BIGINT,
    dropoff_location_id BIGINT,
    payment_type BIGINT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,

    source_file TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.ingestion_audit (
    source_file TEXT PRIMARY KEY,
    source_row_count BIGINT NOT NULL,
    loaded_row_count BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,

    CONSTRAINT chk_ingestion_status
        CHECK (status IN ('STARTED', 'COMPLETED', 'FAILED'))
);


-- ============================================================
-- 3. SILVER LAYER
-- ============================================================
-- Cleaned and standardized taxi trips.
-- bronze_trip_id maintains lineage to the raw layer.
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.taxi_trips (
    silver_trip_id BIGSERIAL PRIMARY KEY,

    bronze_trip_id BIGINT NOT NULL UNIQUE,

    vendor_id INTEGER,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count INTEGER,
    trip_distance NUMERIC(12, 3),
    rate_code_id INTEGER,
    store_and_fwd_flag VARCHAR(1),
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,

    fare_amount NUMERIC(12, 2),
    extra NUMERIC(12, 2),
    mta_tax NUMERIC(12, 2),
    tip_amount NUMERIC(12, 2),
    tolls_amount NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    total_amount NUMERIC(12, 2),
    congestion_surcharge NUMERIC(12, 2),
    airport_fee NUMERIC(12, 2),

    pickup_date DATE NOT NULL,
    pickup_hour SMALLINT NOT NULL,
    trip_duration_minutes NUMERIC(12, 2),

    source_file TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_silver_bronze_trip
        FOREIGN KEY (bronze_trip_id)
        REFERENCES bronze.taxi_trips(bronze_trip_id),

    CONSTRAINT chk_pickup_hour
        CHECK (pickup_hour BETWEEN 0 AND 23),

    CONSTRAINT chk_trip_distance
        CHECK (trip_distance >= 0),

    CONSTRAINT chk_trip_duration
        CHECK (trip_duration_minutes >= 0)
);

CREATE TABLE IF NOT EXISTS silver.rejected_taxi_trips (
    bronze_trip_id BIGINT PRIMARY KEY
        REFERENCES bronze.taxi_trips(bronze_trip_id),
    rejection_reason TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- 4. GOLD DIMENSIONS
-- ============================================================


-- ------------------------------------------------------------
-- Date Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,

    full_date DATE NOT NULL UNIQUE,

    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,

    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,

    is_weekend BOOLEAN NOT NULL
);


-- ------------------------------------------------------------
-- Location Dimension
-- ------------------------------------------------------------
-- source_location_id corresponds to NYC TLC LocationID.
-- The same dimension is used for pickup and dropoff roles.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_location (
    location_key BIGSERIAL PRIMARY KEY,

    source_location_id INTEGER NOT NULL UNIQUE
);


-- ------------------------------------------------------------
-- Payment Type Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_payment_type (
    payment_type_key BIGSERIAL PRIMARY KEY,

    payment_type_code INTEGER NOT NULL UNIQUE,
    payment_type_name VARCHAR(50) NOT NULL
);


-- ------------------------------------------------------------
-- Vendor Dimension
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.dim_vendor (
    vendor_key BIGSERIAL PRIMARY KEY,

    vendor_id INTEGER NOT NULL UNIQUE,
    vendor_name VARCHAR(100) NOT NULL
);


-- ============================================================
-- 5. GOLD FACT TABLE
-- ============================================================
-- Grain:
-- One row represents one NYC Yellow Taxi trip.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_taxi_trips (
    trip_key BIGSERIAL PRIMARY KEY,

    silver_trip_id BIGINT NOT NULL UNIQUE,

    pickup_date_key INTEGER NOT NULL,
    pickup_location_key BIGINT,
    dropoff_location_key BIGINT,
    payment_type_key BIGINT,
    vendor_key BIGINT,

    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_hour SMALLINT NOT NULL,

    passenger_count INTEGER,

    trip_distance NUMERIC(12, 3),
    trip_duration_minutes NUMERIC(12, 2),

    fare_amount NUMERIC(12, 2),
    extra NUMERIC(12, 2),
    mta_tax NUMERIC(12, 2),
    tip_amount NUMERIC(12, 2),
    tolls_amount NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    congestion_surcharge NUMERIC(12, 2),
    airport_fee NUMERIC(12, 2),
    total_amount NUMERIC(12, 2),

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_silver_trip
        FOREIGN KEY (silver_trip_id)
        REFERENCES silver.taxi_trips(silver_trip_id),

    CONSTRAINT fk_fact_pickup_date
        FOREIGN KEY (pickup_date_key)
        REFERENCES gold.dim_date(date_key),

    CONSTRAINT fk_fact_pickup_location
        FOREIGN KEY (pickup_location_key)
        REFERENCES gold.dim_location(location_key),

    CONSTRAINT fk_fact_dropoff_location
        FOREIGN KEY (dropoff_location_key)
        REFERENCES gold.dim_location(location_key),

    CONSTRAINT fk_fact_payment_type
        FOREIGN KEY (payment_type_key)
        REFERENCES gold.dim_payment_type(payment_type_key),

    CONSTRAINT fk_fact_vendor
        FOREIGN KEY (vendor_key)
        REFERENCES gold.dim_vendor(vendor_key),

    CONSTRAINT chk_fact_pickup_hour
        CHECK (pickup_hour BETWEEN 0 AND 23),

    CONSTRAINT chk_fact_trip_distance
        CHECK (trip_distance >= 0),

    CONSTRAINT chk_fact_trip_duration
        CHECK (trip_duration_minutes >= 0)
);


-- ============================================================
-- 6. INDEXES
-- ============================================================
-- These indexes support common filtering, joins and dashboard
-- analytical queries.
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_bronze_source_file
    ON bronze.taxi_trips(source_file);


CREATE INDEX IF NOT EXISTS idx_silver_pickup_datetime
    ON silver.taxi_trips(pickup_datetime);


CREATE INDEX IF NOT EXISTS idx_silver_pickup_date
    ON silver.taxi_trips(pickup_date);


CREATE INDEX IF NOT EXISTS idx_fact_pickup_date_key
    ON gold.fact_taxi_trips(pickup_date_key);


CREATE INDEX IF NOT EXISTS idx_fact_payment_type_key
    ON gold.fact_taxi_trips(payment_type_key);


CREATE INDEX IF NOT EXISTS idx_fact_vendor_key
    ON gold.fact_taxi_trips(vendor_key);


CREATE INDEX IF NOT EXISTS idx_fact_pickup_location_key
    ON gold.fact_taxi_trips(pickup_location_key);


CREATE INDEX IF NOT EXISTS idx_fact_dropoff_location_key
    ON gold.fact_taxi_trips(dropoff_location_key);


CREATE INDEX IF NOT EXISTS idx_fact_pickup_hour
    ON gold.fact_taxi_trips(pickup_hour);