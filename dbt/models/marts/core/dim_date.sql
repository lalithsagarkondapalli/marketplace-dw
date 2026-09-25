select
    cast(to_char(d, 'YYYYMMDD') as integer) as date_key,
    cast(d as date)                         as calendar_date,
    extract(year from d)::int               as year,
    extract(quarter from d)::int            as quarter,
    extract(month from d)::int              as month,
    to_char(d, 'Mon')                       as month_name,
    cast(date_trunc('month', d) as date)    as month_start,
    extract(isodow from d)::int             as iso_day_of_week,
    extract(isodow from d) in (6, 7)        as is_weekend
from generate_series('2016-01-01'::date, '2018-12-31'::date, interval '1 day') as d
