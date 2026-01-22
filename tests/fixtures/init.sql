-- PipeAudit Test Database Setup
-- This file is executed on ClickHouse container startup

-- Create test database
CREATE DATABASE IF NOT EXISTS testdb;

-- Scenario 1: Healthy table with normal parts count
CREATE TABLE testdb.events (
    event_date Date,
    event_time DateTime,
    user_id UInt64,
    event_type LowCardinality(String),
    payload String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id, event_time);

-- Insert test data for healthy table
INSERT INTO testdb.events
SELECT
    today() - (number % 30) AS event_date,
    now() - (number * 60) AS event_time,
    rand() % 10000 AS user_id,
    ['click', 'view', 'purchase'][(number % 3) + 1] AS event_type,
    concat('payload_', toString(number)) AS payload
FROM numbers(10000);

-- Scenario 2: Table for MV chain testing
CREATE TABLE testdb.events_raw (
    event_date Date,
    event_time DateTime,
    user_id UInt64,
    event_type String,
    value Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, user_id);

-- Materialized View: Daily aggregation
CREATE MATERIALIZED VIEW testdb.events_daily_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_type)
AS SELECT
    event_date,
    event_type,
    count() AS event_count,
    sum(value) AS total_value
FROM testdb.events_raw
GROUP BY event_date, event_type;

-- Insert data into raw table (will populate MV)
INSERT INTO testdb.events_raw
SELECT
    today() - (number % 7) AS event_date,
    now() - (number * 300) AS event_time,
    rand() % 1000 AS user_id,
    ['sale', 'refund', 'view'][(number % 3) + 1] AS event_type,
    (rand() % 10000) / 100.0 AS value
FROM numbers(5000);
