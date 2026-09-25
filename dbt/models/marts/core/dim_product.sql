select
    {{ surrogate_key(['product_id']) }} as product_key,
    product_id,
    category_name,
    category_name_pt,
    product_photos_qty,
    product_weight_g,
    product_length_cm * product_height_cm * product_width_cm as product_volume_cm3
from {{ ref('stg_products') }}
