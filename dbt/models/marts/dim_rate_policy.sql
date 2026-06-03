select
    dbt_scd_id              as rate_policy_key,
    policy_code,
    policy_name,
    amount,
    currency,
    dbt_valid_from          as valid_from,
    dbt_valid_to            as valid_to,
    (dbt_valid_to is null)  as is_current
from {{ ref('snap_rate_policy') }}
