with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

cleaned as (

    select *
    from trips
    where pickup_at >= '2024-01-01'
      and pickup_at <  '2024-04-01'
      and dropoff_at >= pickup_at
      and datediff('minute', pickup_at, dropoff_at) between 1 and 1440
      and trip_distance > 0
      and fare_amount  >= 0
      and total_amount >= 0

),

enriched as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id', 'pickup_at', 'dropoff_at',
            'pickup_location_id', 'dropoff_location_id', 'total_amount'
        ]) }}                                          as trip_key,

        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        passenger_count,

        pickup_at,
        dropoff_at,
        cast(pickup_at as date)                        as pickup_date,
        date_part('hour', pickup_at)                   as pickup_hour,
        dayofweekiso(pickup_at)                        as pickup_dow_iso,
        datediff('minute', pickup_at, dropoff_at)      as trip_duration_min,

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

    from cleaned
    -- dedup exact duplicate trips on the surrogate key
    qualify row_number() over (partition by trip_key order by pickup_at) = 1

)

select * from enriched
