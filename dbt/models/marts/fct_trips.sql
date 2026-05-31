select
    trip_key,

    -- foreign keys
    pickup_date,
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type_id,

    -- degenerate / context
    pickup_at,
    dropoff_at,
    pickup_hour,
    pickup_dow_iso,
    passenger_count,
    trip_duration_min,

    -- measures
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount

from {{ ref('int_trips_enriched') }}
