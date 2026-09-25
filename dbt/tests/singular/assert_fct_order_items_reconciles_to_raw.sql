-- Row count and item-price total in the fact must equal the raw source exactly.
with raw_totals as (
    select count(*) as row_count, sum(cast(price as numeric(12,2))) as price_total
    from {{ source('raw', 'order_items') }}
),
fact_totals as (
    select count(*) as row_count, sum(item_price) as price_total
    from {{ ref('fct_order_items') }}
)
select r.*, f.row_count as fact_rows, f.price_total as fact_price_total
from raw_totals r cross join fact_totals f
where r.row_count <> f.row_count or r.price_total <> f.price_total
