-- The point-in-time join must attach the customer version whose address matches
-- the address recorded on that order.
select f.order_item_key
from {{ ref('fct_order_items') }} f
join {{ ref('stg_orders') }} o     on o.order_id = f.order_id
join {{ ref('stg_customers') }} oc on oc.customer_id = o.customer_id
join {{ ref('dim_customer') }} c   on c.customer_sk = f.customer_sk
where (oc.zip_code_prefix, oc.city, oc.state) is distinct from (c.zip_code_prefix, c.city, c.state)
