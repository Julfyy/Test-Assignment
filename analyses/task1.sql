with monthly_revenue as (
    select
        date_trunc('month', order_date_kyiv::timestamp) as "month",
        sum(total_revenue) as total_revenue
    from {{ ref('fct_sales') }}
    group by "month"
)

select
    "month",
    ROUND(total_revenue::numeric, 2) as total_revenue,
    ROUND(lag(total_revenue) over (order by month)::numeric, 2) as previous_month_revenue,
    ROUND(((total_revenue - lag(total_revenue) over (order by month)) / lag(total_revenue) over (order by month) * 100)::numeric, 1) as revenue_growth_percent
from monthly_revenue
order by "month";
