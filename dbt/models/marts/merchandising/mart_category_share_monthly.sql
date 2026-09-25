-- Each category's share of monthly GMV and its month-over-month growth.
with monthly as (
    select month_start, category_name, gmv, units_sold, orders
    from {{ ref('mart_category_monthly') }}
    where gmv is not null
)

select
    month_start,
    category_name,
    gmv,
    units_sold,
    orders,
    round(gmv / sum(gmv) over (partition by month_start), 4)            as gmv_share,
    rank() over (partition by month_start order by gmv desc)             as gmv_rank,
    lag(gmv) over (partition by category_name order by month_start)      as prior_month_gmv,
    round((gmv - lag(gmv) over (partition by category_name order by month_start))
          / nullif(lag(gmv) over (partition by category_name order by month_start), 0), 4)
                                                                         as gmv_mom_growth
from monthly
