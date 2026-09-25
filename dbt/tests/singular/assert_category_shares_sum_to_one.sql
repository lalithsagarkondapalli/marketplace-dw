-- Within each month, category GMV shares must add up to 1 (allowing rounding).
select month_start, sum(gmv_share) as total_share
from {{ ref('mart_category_share_monthly') }}
group by 1
having abs(sum(gmv_share) - 1) > 0.01
