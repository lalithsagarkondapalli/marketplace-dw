-- Monthly category performance. Metric definitions are documented in _marts.yml.
-- Revenue metrics exclude canceled and unavailable orders. An order containing
-- products from several categories counts once in each of those categories.
with lines as (
    select
        d.month_start,
        p.category_name,
        f.order_id,
        f.item_price,
        f.freight_value,
        f.order_status not in ('canceled', 'unavailable') as is_revenue
    from {{ ref('fct_order_items') }} f
    join {{ ref('dim_date') }} d    on d.date_key = f.purchase_date_key
    join {{ ref('dim_product') }} p on p.product_key = f.product_key
),

line_metrics as (
    select
        month_start,
        category_name,
        count(*) filter (where is_revenue)              as units_sold,
        sum(item_price) filter (where is_revenue)       as gmv,
        sum(freight_value) filter (where is_revenue)    as freight_revenue,
        round(avg(item_price), 2)                       as avg_item_price
    from lines
    group by 1, 2
),

order_metrics as (
    select
        l.month_start,
        l.category_name,
        count(*)                                                         as orders,
        round(avg(case when o.is_late then 1.0 when not o.is_late then 0.0 end), 4)
                                                                         as late_delivery_rate,
        round(avg(o.review_score), 2)                                    as avg_review_score,
        round(avg(case when o.order_status = 'canceled' then 1.0 else 0.0 end), 4)
                                                                         as cancel_rate
    from (select distinct month_start, category_name, order_id from lines) l
    join {{ ref('fct_orders') }} o on o.order_id = l.order_id
    group by 1, 2
)

select
    lm.month_start,
    lm.category_name,
    om.orders,
    lm.units_sold,
    lm.gmv,
    lm.freight_revenue,
    lm.avg_item_price,
    om.late_delivery_rate,
    om.avg_review_score,
    om.cancel_rate
from line_metrics lm
join order_metrics om using (month_start, category_name)
