-- One row per order: the address a person (customer_unique_id) used at purchase time.
-- This is the change history that dim_customer turns into SCD Type 2 versions.
select
    c.customer_unique_id,
    c.customer_id,
    c.zip_code_prefix,
    c.city,
    c.state,
    o.purchased_at as observed_at
from {{ ref('stg_customers') }} c
join {{ ref('stg_orders') }} o
    on o.customer_id = c.customer_id
