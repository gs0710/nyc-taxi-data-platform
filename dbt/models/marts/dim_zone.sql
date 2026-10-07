-- dbt reads the text "N/A" in a seed as NULL, so we put it back for clarity.
select
    location_id                       as zone_key,
    coalesce(borough, 'N/A')          as borough,
    coalesce(zone, 'N/A')             as zone_name,
    coalesce(service_zone, 'N/A')     as service_zone
from {{ ref('taxi_zone_lookup') }}
