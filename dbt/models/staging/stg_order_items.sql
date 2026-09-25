select
    {{ surrogate_key(['order_id', 'order_item_id']) }} as order_item_key,
    order_id,
    {{ to_int('order_item_id') }}                      as order_item_seq,
    product_id,
    seller_id,
    {{ to_ts('shipping_limit_date') }}                 as shipping_limit_at,
    {{ to_num('price') }}                              as item_price,
    {{ to_num('freight_value') }}                      as freight_value,
    _batch_id,
    _loaded_at
from {{ source('raw', 'order_items') }}
