with source as (

    select * from {{ source('raw', 'taxi_zone_lookup') }}

)

select
    locationid::int      as zone_id,
    borough::string      as borough,
    zone::string         as zone_name,
    service_zone::string as service_zone
from source
