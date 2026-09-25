select
    {{ surrogate_key(['s.seller_id']) }} as seller_key,
    s.seller_id,
    s.zip_code_prefix,
    s.city,
    s.state,
    g.latitude,
    g.longitude
from {{ ref('stg_sellers') }} s
left join {{ ref('stg_geolocation') }} g using (zip_code_prefix)
