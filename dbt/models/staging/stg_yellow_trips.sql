with source as (

    select record, _source_file, _loaded_at
    from {{ source('raw', 'yellow_tripdata_raw') }}

),

renamed as (

    select
        -- dimension keys
        record:"VendorID"::int                                      as vendor_id,
        record:"RatecodeID"::int                                    as rate_code_id,
        record:"PULocationID"::int                                  as pickup_location_id,
        record:"DOLocationID"::int                                  as dropoff_location_id,
        record:"payment_type"::int                                  as payment_type_id,
        record:"store_and_fwd_flag"::string                         as store_and_fwd_flag,
        record:"passenger_count"::int                               as passenger_count,

        -- timestamps: epoch MICROSECONDS -> timestamp_ntz (scale 6)
        to_timestamp_ntz(record:"tpep_pickup_datetime"::number, 6)  as pickup_at,
        to_timestamp_ntz(record:"tpep_dropoff_datetime"::number, 6) as dropoff_at,

        -- measures
        record:"trip_distance"::number(10,2)                        as trip_distance,
        record:"fare_amount"::number(10,2)                          as fare_amount,
        record:"extra"::number(10,2)                                as extra,
        record:"mta_tax"::number(10,2)                              as mta_tax,
        record:"tip_amount"::number(10,2)                           as tip_amount,
        record:"tolls_amount"::number(10,2)                         as tolls_amount,
        record:"improvement_surcharge"::number(10,2)                as improvement_surcharge,
        record:"congestion_surcharge"::number(10,2)                 as congestion_surcharge,
        record:"Airport_fee"::number(10,2)                          as airport_fee,
        record:"total_amount"::number(10,2)                         as total_amount,

        -- lineage
        _source_file                                                as source_file,
        _loaded_at                                                  as loaded_at

    from source

)

select * from renamed
