-- Spark quarantines trips over 100 miles.
select * from {{ ref('fact_trips') }} where trip_distance > 100
