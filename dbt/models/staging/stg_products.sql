-- Source column names are misspelled upstream ("lenght"); renamed here.
select
    p.product_id,
    nullif(trim(p.product_category_name), '')                         as category_name_pt,
    coalesce(t.product_category_name_english,
             nullif(trim(p.product_category_name), ''), 'unknown')    as category_name,
    t.product_category_name is not null                               as has_category_translation,
    {{ to_int('p.product_name_lenght') }}                             as product_name_length,
    {{ to_int('p.product_description_lenght') }}                      as product_description_length,
    {{ to_int('p.product_photos_qty') }}                              as product_photos_qty,
    {{ to_int('p.product_weight_g') }}                                as product_weight_g,
    {{ to_int('p.product_length_cm') }}                               as product_length_cm,
    {{ to_int('p.product_height_cm') }}                               as product_height_cm,
    {{ to_int('p.product_width_cm') }}                                as product_width_cm
from {{ source('raw', 'products') }} p
left join {{ source('raw', 'category_translation') }} t
    on trim(p.product_category_name) = trim(t.product_category_name)
