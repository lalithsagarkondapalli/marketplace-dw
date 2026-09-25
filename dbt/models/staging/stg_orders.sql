select
    order_id,
    customer_id,
    lower(trim(order_status))                         as order_status,
    {{ to_ts('order_purchase_timestamp') }}           as purchased_at,
    {{ to_ts('order_approved_at') }}                  as approved_at,
    {{ to_ts('order_delivered_carrier_date') }}       as delivered_to_carrier_at,
    {{ to_ts('order_delivered_customer_date') }}      as delivered_to_customer_at,
    {{ to_ts('order_estimated_delivery_date') }}      as estimated_delivery_at,
    _batch_id,
    _loaded_at
from {{ source('raw', 'orders') }}
