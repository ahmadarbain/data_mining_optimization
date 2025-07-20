CREATE DATABASE IF NOT EXISTS coal_mining;
USE coal_mining;

CREATE TABLE IF NOT EXISTS production_logs (
    log_id UInt32,
    date Date,
    mine_id UInt32,
    shift String,
    tons_extracted Float64,
    quality_grade Float32
) ENGINE = MergeTree()
ORDER BY (date, mine_id);

CREATE TABLE IF NOT EXISTS daily_production_metrics (
    date Date,
    mine_id UInt32,
    total_production_daily Float64,
    average_quality_grade Float32,
    equipment_utilization Float32,
    fuel_efficiency Float32,
    rainfall_mm Float32
) ENGINE = MergeTree()
ORDER BY (date, mine_id);
