-- Spark quarantines negative fares, so none should reach the fact table.
select * from {{ ref('fact_trips') }} where fare_amount < 0
