-- customer_id is per order; customer_unique_id identifies the person.
select
    customer_id,
    customer_unique_id,
    lpad(trim(customer_zip_code_prefix), 5, '0') as zip_code_prefix,
    lower(trim(customer_city))                   as city,
    upper(trim(customer_state))                  as state,
    _loaded_at
from {{ source('raw', 'customers') }}
