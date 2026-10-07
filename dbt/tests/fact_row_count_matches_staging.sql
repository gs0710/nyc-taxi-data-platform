-- Fails (returns a row) if the fact table lost or gained rows compared with staging.
select s.n as staging_rows, f.n as fact_rows
from (select count(*) as n from {{ ref('stg_yellow_trips') }}) s,
     (select count(*) as n from {{ ref('fact_trips') }}) f
where s.n <> f.n
