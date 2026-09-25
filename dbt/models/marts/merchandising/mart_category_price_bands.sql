-- Units, GMV, delivery and review outcomes by category and item price band.
-- Revenue order lines only (canceled/unavailable excluded).
with lines as (
    select
        p.category_name,
        f.order_id,
        f.item_price,
        case
            when f.item_price < 25   then '1: <25'
            when f.item_price < 50   then '2: 25-49'
            when f.item_price < 100  then '3: 50-99'
            when f.item_price < 250  then '4: 100-249'
            else                          '5: 250+'
        end as price_band
    from {{ ref('fct_order_items') }} f
    join {{ ref('dim_product') }} p on p.product_key = f.product_key
    where f.order_status not in ('canceled', 'unavailable')
)

select
    l.category_name,
    l.price_band,
    count(*)                                                     as units_sold,
    count(distinct l.order_id)                                   as orders,
    sum(l.item_price)                                            as gmv,
    round(avg(l.item_price), 2)                                  as avg_item_price,
    round(avg(o.review_score), 2)                                as avg_review_score,
    round(avg(case when o.is_late then 1.0 when not o.is_late then 0.0 end), 4)
                                                                 as late_delivery_rate
from lines l
join {{ ref('fct_orders') }} o on o.order_id = l.order_id
group by 1, 2
