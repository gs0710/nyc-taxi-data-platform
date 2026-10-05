-- Staging table: cleaned trips as produced by spark/clean_trips.py.
-- Safe to run more than once (IF NOT EXISTS).
CREATE TABLE IF NOT EXISTS staging.yellow_trips (
    vendor_id               integer,
    pickup_datetime         timestamp        NOT NULL,
    dropoff_datetime        timestamp        NOT NULL,
    passenger_count         integer,
    trip_distance           double precision,
    rate_code_id            integer,
    store_and_fwd_flag      text,
    pickup_location_id      integer,
    dropoff_location_id     integer,
    payment_type            integer,
    fare_amount             numeric(12,2),
    extra                   numeric(12,2),
    mta_tax                 numeric(12,2),
    tip_amount              numeric(12,2),
    tolls_amount            numeric(12,2),
    improvement_surcharge   numeric(12,2),
    total_amount            numeric(12,2),
    congestion_surcharge    numeric(12,2),
    airport_fee             numeric(12,2),
    cbd_congestion_fee      numeric(12,2),
    pickup_borough          text,
    pickup_zone             text,
    dropoff_borough         text,
    dropoff_zone            text,
    passenger_count_missing boolean          NOT NULL,
    zero_distance           boolean          NOT NULL,
    trip_duration_minutes   double precision,
    year                    smallint         NOT NULL,
    month                   smallint         NOT NULL,
    loaded_at               timestamptz      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_yellow_trips_year_month
    ON staging.yellow_trips (year, month);

-- One row per load attempt: what was loaded, how much, and whether it worked.
CREATE TABLE IF NOT EXISTS audit.load_history (
    load_id          bigserial   PRIMARY KEY,
    dataset          text        NOT NULL,
    source_month     char(7)     NOT NULL,
    rows_in_parquet  bigint,
    rows_loaded      bigint,
    status           text        NOT NULL CHECK (status IN ('success', 'failed')),
    error_message    text,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL
);
