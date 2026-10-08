-- Spark rejects trips that end before they start.
select * from {{ ref('fact_trips') }} where dropoff_datetime < pickup_datetime
