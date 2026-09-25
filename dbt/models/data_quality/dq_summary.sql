select
    entity,
    rule,
    count(*)            as failing_rows,
    current_timestamp   as measured_at
from {{ ref('dq_issues') }}
group by 1, 2
order by 3 desc
