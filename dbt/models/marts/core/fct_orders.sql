-- Accumulating-snapshot fact: one row per order with its lifecycle milestones,
-- item/payment totals, delivery performance, and data-quality flags.
select
    o.order_id,
    cast(to_char(o.purchased_at, 'YYYYMMDD') as integer)                as purchase_date_key,
    cast(to_char(o.delivered_to_customer_at, 'YYYYMMDD') as integer)    as delivered_date_key,
    c.customer_unique_id,
    o.order_status,
    o.purchased_at,
    o.approved_at,
    o.delivered_to_carrier_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,
    coalesce(t.item_count, 0)                                           as item_count,
    coalesce(t.items_value, 0)                                          as items_value,
    coalesce(t.freight_value, 0)                                        as freight_value,
    p.payment_value,
    p.payment_types,
    r.review_score,
    extract(epoch from (o.delivered_to_customer_at - o.purchased_at)) / 86400.0
                                                                        as delivery_days,
    case when o.delivered_to_customer_at is null then null
         else o.delivered_to_customer_at::date > o.estimated_delivery_at::date
    end                                                                 as is_late,
    t.order_id is null                                                  as dq_no_items,
    abs(coalesce(p.payment_value, 0)
        - coalesce(t.items_value, 0) - coalesce(t.freight_value, 0)) > 1.00
                                                                        as dq_payment_mismatch
from {{ ref('stg_orders') }} o
join {{ ref('stg_customers') }} c        on c.customer_id = o.customer_id
left join {{ ref('int_order_item_totals') }} t on t.order_id = o.order_id
left join {{ ref('int_order_payments') }} p    on p.order_id = o.order_id
left join {{ ref('stg_order_reviews') }} r     on r.order_id = o.order_id
