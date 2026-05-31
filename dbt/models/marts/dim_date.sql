with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2024-04-01' as date)"
    ) }}

),

dates as (
    select cast(date_day as date) as date_key from spine
)

select
    date_key,
    year(date_key)        as year,
    quarter(date_key)     as quarter,
    month(date_key)       as month,
    monthname(date_key)   as month_name,
    day(date_key)         as day_of_month,
    dayofweekiso(date_key) as day_of_week_iso,
    dayname(date_key)     as day_name,
    iff(dayofweekiso(date_key) in (6, 7), true, false) as is_weekend
from dates
