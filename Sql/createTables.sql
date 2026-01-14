-- Hourly consumption – one row per project‑hour
CREATE TABLE consumption_hourly (
    project_id TEXT NOT NULL,
    timeframe_start TIMESTAMPTZ NOT NULL,
    timeframe_end       TIMESTAMPTZ NOT NULL,
    active_time_seconds BIGINT,
    compute_time_seconds BIGINT,
    written_data_bytes BIGINT,
    synthetic_storage_size_bytes bigint,
  primary key (project_id, timeframe_start, timeframe_end));

-- Daily consumption – one row per project‑day
CREATE TABLE consumption_daily (
    project_id TEXT NOT NULL,
    timeframe_start TIMESTAMPTZ NOT NULL,
    timeframe_end       TIMESTAMPTZ NOT NULL,
    active_time_seconds BIGINT,
    compute_time_seconds BIGINT,
    written_data_bytes BIGINT,
    synthetic_storage_size_bytes bigint,
  primary key (project_id, timeframe_start, timeframe_end));

-- Monthly consumption – one row per project‑month
CREATE TABLE consumption_monthly (
    project_id TEXT NOT NULL,
    timeframe_start TIMESTAMPTZ NOT NULL,
    timeframe_end       TIMESTAMPTZ NOT NULL,
    active_time_seconds BIGINT,
    compute_time_seconds BIGINT,
    written_data_bytes BIGINT,
    synthetic_storage_size_bytes bigint,
  primary key (project_id, timeframe_start, timeframe_end));
