select customer_unique_id, count(*) as current_versions
from {{ ref('dim_customer') }}
where is_current
group by 1
having count(*) <> 1
