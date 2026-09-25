-- Row-level data-quality findings with a reason code. Rows are flagged, not deleted:
-- facts keep them (with dq_* flags) so totals still reconcile to the source.
select 'order' as entity, order_id as entity_id, 'delivered_before_purchase' as rule
from {{ ref('stg_orders') }}
where delivered_to_customer_at < purchased_at

union all
select 'order', order_id, 'delivered_status_without_delivery_date'
from {{ ref('stg_orders') }}
where order_status = 'delivered' and delivered_to_customer_at is null

union all
select 'order', order_id, 'carrier_pickup_before_approval'
from {{ ref('stg_orders') }}
where delivered_to_carrier_at < approved_at

union all
select 'order', order_id, 'order_without_items'
from {{ ref('fct_orders') }}
where dq_no_items

union all
select 'order', order_id, 'payment_differs_from_items_plus_freight'
from {{ ref('fct_orders') }}
where dq_payment_mismatch and not dq_no_items

union all
select 'product', product_id, 'product_missing_category'
from {{ ref('stg_products') }}
where category_name_pt is null

union all
select 'product', product_id, 'category_missing_translation'
from {{ ref('stg_products') }}
where category_name_pt is not null and not has_category_translation

union all
select 'seller', seller_id, 'seller_zip_not_in_geolocation'
from {{ ref('dim_seller') }}
where latitude is null
