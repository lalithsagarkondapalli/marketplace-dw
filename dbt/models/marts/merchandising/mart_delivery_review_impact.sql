-- How delivery timing relative to the promised date relates to review scores.
with delivered as (
    select
        review_score,
        (delivered_to_customer_at::date - estimated_delivery_at::date) as days_vs_estimate
    from {{ ref('fct_orders') }}
    where delivered_to_customer_at is not null
      and review_score is not null
)

select
    case
        when days_vs_estimate <= -7 then '1: 7+ days early'
        when days_vs_estimate <= 0  then '2: early or on time'
        when days_vs_estimate <= 3  then '3: 1-3 days late'
        when days_vs_estimate <= 7  then '4: 4-7 days late'
        else                             '5: 8+ days late'
    end                                                        as delivery_bucket,
    count(*)                                                   as orders,
    round(avg(review_score), 2)                                as avg_review_score,
    round(avg(case when review_score <= 2 then 1.0 else 0.0 end), 4) as low_review_rate
from delivered
group by 1
order by 1
