-- Portable, intentionally modest relational warehouse for local SQLite and PostgreSQL.
-- Dates are normalized UTC ISO-8601 strings to support both backends identically.
CREATE TABLE IF NOT EXISTS dim_city (
  city_id TEXT PRIMARY KEY, city TEXT NOT NULL, state TEXT NOT NULL,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id TEXT PRIMARY KEY, city_id TEXT NOT NULL REFERENCES dim_city(city_id)
);
CREATE TABLE IF NOT EXISTS dim_driver (
  driver_id TEXT PRIMARY KEY, city_id TEXT NOT NULL REFERENCES dim_city(city_id)
);
CREATE TABLE IF NOT EXISTS stg_orders (
  order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, driver_id TEXT, city_id TEXT NOT NULL,
  created_at TEXT NOT NULL, picked_up_at TEXT, promised_at TEXT NOT NULL, delivered_at TEXT,
  status TEXT NOT NULL, distance_km DOUBLE PRECISION, fare_inr DOUBLE PRECISION, source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stg_events (
  event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, event_type TEXT NOT NULL,
  event_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_delivery (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL REFERENCES dim_customer(customer_id),
  driver_id TEXT REFERENCES dim_driver(driver_id),
  city_id TEXT NOT NULL REFERENCES dim_city(city_id),
  created_at TEXT NOT NULL, picked_up_at TEXT, promised_at TEXT NOT NULL, delivered_at TEXT,
  status TEXT NOT NULL, distance_km DOUBLE PRECISION, fare_inr DOUBLE PRECISION,
  on_time INTEGER, delay_minutes DOUBLE PRECISION, source TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_delivery_event (
  event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL REFERENCES fact_delivery(order_id),
  event_type TEXT NOT NULL, event_at TEXT NOT NULL, ingested_at TEXT NOT NULL,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_delivery_city_created ON fact_delivery(city_id, created_at);
CREATE INDEX IF NOT EXISTS ix_delivery_status ON fact_delivery(status);
CREATE INDEX IF NOT EXISTS ix_event_order_ts ON fact_delivery_event(order_id, event_at);
CREATE TABLE IF NOT EXISTS pipeline_audit (
  run_id TEXT PRIMARY KEY, run_started TEXT NOT NULL, run_finished TEXT NOT NULL,
  source_orders INTEGER NOT NULL, source_events INTEGER NOT NULL, duplicates_removed INTEGER NOT NULL,
  inserted_orders INTEGER NOT NULL, inserted_events INTEGER NOT NULL, quality_status TEXT NOT NULL
);
