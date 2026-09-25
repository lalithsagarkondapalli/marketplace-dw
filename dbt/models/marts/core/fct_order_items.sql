{{ config(
    materialized='incremental',
    unique_key='order_item_key',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

-- Grain: one row per order line.
-- Incremental on the raw-load watermark: only rows loaded after the newest
-- _loaded_at already in the fact are processed. Re-running a month reloads its
-- raw rows with a fresh _loaded_at, so they are picked up again and replace the
-- old fact rows through delete+insert on order_item_key.
with items as (
    select *
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select coalesce(max(_loaded_at), '1900-01-01'::timestamptz) from {{ this }}
    )
    {% endif %}
)

select
    i.order_item_key,
    i.order_id,
    i.order_item_seq,
    cast(to_char(o.purchased_at, 'YYYYMMDD') as integer) as purchase_date_key,
    p.product_key,
    s.seller_key,
    c.customer_sk,
    o.order_status,
    i.item_price,
    i.freight_value,
    cast(i.item_price + i.freight_value as numeric(12,2)) as gross_item_value,
    o.purchased_at,
    i._loaded_at
from items i
join {{ ref('stg_orders') }} o
    on o.order_id = i.order_id
join {{ ref('stg_customers') }} oc
    on oc.customer_id = o.customer_id
left join {{ ref('dim_product') }} p
    on p.product_id = i.product_id
left join {{ ref('dim_seller') }} s
    on s.seller_id = i.seller_id
-- point-in-time join: the customer version valid when the order was placed
left join {{ ref('dim_customer') }} c
    on c.customer_unique_id = oc.customer_unique_id
   and o.purchased_at >= c.valid_from
   and (c.valid_to is null or o.purchased_at < c.valid_to)
