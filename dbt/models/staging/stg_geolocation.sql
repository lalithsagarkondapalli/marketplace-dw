-- ~1M rows with many duplicate points per zip prefix and some coordinates outside
-- Brazil. Keep points inside Brazil's bounding box and collapse to one centroid
-- per zip prefix.
select
    lpad(trim(geolocation_zip_code_prefix), 5, '0') as zip_code_prefix,
    round(avg(cast(geolocation_lat as numeric)), 6)  as latitude,
    round(avg(cast(geolocation_lng as numeric)), 6)  as longitude,
    count(*)                                         as source_points
from {{ source('raw', 'geolocation') }}
where cast(geolocation_lat as numeric) between -33.8 and 5.3
  and cast(geolocation_lng as numeric) between -73.99 and -34.8
group by 1
