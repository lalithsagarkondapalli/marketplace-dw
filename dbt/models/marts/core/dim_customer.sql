-- SCD Type 2 on business time: a new version starts whenever a person's address
-- (zip prefix, city, state) differs from the address on their previous order.
-- valid_to is exclusive; the current version has valid_to = null.
with obs as (
    select
        customer_unique_id,
        zip_code_prefix,
        city,
        state,
        observed_at,
        zip_code_prefix || '|' || city || '|' || state as address_hash
    from {{ ref('int_customer_address_observations') }}
),

flagged as (
    select
        *,
        lag(address_hash) over (
            partition by customer_unique_id order by observed_at, address_hash
        ) as prev_address_hash
    from obs
),

versions as (
    select *
    from flagged
    where prev_address_hash is distinct from address_hash
)

select
    {{ surrogate_key(['customer_unique_id', 'observed_at', 'address_hash']) }} as customer_sk,
    customer_unique_id,
    zip_code_prefix,
    city,
    state,
    observed_at as valid_from,
    lead(observed_at) over (
        partition by customer_unique_id order by observed_at, address_hash
    ) as valid_to,
    lead(observed_at) over (
        partition by customer_unique_id order by observed_at, address_hash
    ) is null as is_current,
    row_number() over (
        partition by customer_unique_id order by observed_at, address_hash
    ) as version_number
from versions
