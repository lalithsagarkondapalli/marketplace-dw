-- The source has orders with more than one review and review_ids reused across
-- orders. Keep the most recently answered review per order.
with ranked as (
    select
        review_id,
        order_id,
        {{ to_int('review_score') }}          as review_score,
        {{ to_ts('review_creation_date') }}   as review_created_at,
        {{ to_ts('review_answer_timestamp') }} as review_answered_at,
        row_number() over (
            partition by order_id
            order by {{ to_ts('review_answer_timestamp') }} desc nulls last, review_id
        ) as rn
    from {{ source('raw', 'order_reviews') }}
)
select review_id, order_id, review_score, review_created_at, review_answered_at
from ranked
where rn = 1
