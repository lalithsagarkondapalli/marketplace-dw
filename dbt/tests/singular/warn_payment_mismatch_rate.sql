-- Warn (do not fail) when more than 1% of orders with items have payments that
-- differ from items + freight by more than 1.00 BRL.
{{ config(severity='warn') }}
select mismatch_rate
from (
    select avg(case when dq_payment_mismatch then 1.0 else 0.0 end) as mismatch_rate
    from {{ ref('fct_orders') }}
    where not dq_no_items
) s
where mismatch_rate > 0.01
