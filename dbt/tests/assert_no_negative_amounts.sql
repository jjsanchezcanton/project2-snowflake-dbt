-- Singular test: no negative monetary measures or distance in the fact table.
-- Expected to pass (intermediate already filters these out) — acts as a guard
-- against regressions in the cleaning logic.
select
    trip_key,
    fare_amount,
    total_amount,
    trip_distance
from {{ ref('fct_trips') }}
where fare_amount  < 0
   or total_amount < 0
   or trip_distance < 0
