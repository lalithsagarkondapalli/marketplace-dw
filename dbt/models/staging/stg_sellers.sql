select
    seller_id,
    lpad(trim(seller_zip_code_prefix), 5, '0') as zip_code_prefix,
    lower(trim(seller_city))                   as city,
    upper(trim(seller_state))                  as state
from {{ source('raw', 'sellers') }}
