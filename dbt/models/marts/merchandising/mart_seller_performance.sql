-- Seller scorecard: volume, fulfilment speed, lateness, reviews and cancellations.
with seller_orders as (
    select distinct
        s.seller_id,
        s.state as seller_state,
        f.order_id
    from {{ ref('fct_order_items') }} f
    join {{ ref('dim_seller') }} s on s.seller_key = f.seller_key
),

seller_lines as (
    select
        s.seller_id,
        count(*) filter (where f.order_status not in ('canceled', 'unavailable'))           as units_sold,
        sum(f.item_price) filter (where f.order_status not in ('canceled', 'unavailable'))  as gmv,
        count(distinct p.category_name)                                                      as categories_sold
    from {{ ref('fct_order_items') }} f
    join {{ ref('dim_seller') }} s  on s.seller_key = f.seller_key
    join {{ ref('dim_product') }} p on p.product_key = f.product_key
    group by 1
)

select
    so.seller_id,
    so.seller_state,
    count(*)                                                                 as orders,
    sl.units_sold,
    sl.gmv,
    sl.categories_sold,
    round(avg(o.delivery_days)::numeric, 2)                                  as avg_delivery_days,
    round(avg(case when o.is_late then 1.0 when not o.is_late then 0.0 end), 4) as late_delivery_rate,
    round(avg(o.review_score), 2)                                            as avg_review_score,
    round(avg(case when o.order_status = 'canceled' then 1.0 else 0.0 end), 4) as cancel_rate,
    ntile(4) over (order by sl.gmv desc nulls last)                          as gmv_quartile
from seller_orders so
join {{ ref('fct_orders') }} o using (order_id)
join seller_lines sl using (seller_id)
group by so.seller_id, so.seller_state, sl.units_sold, sl.gmv, sl.categories_sold
