with exploded_agents as (
    select
        "Reference ID",
        unnest("sales_agent_list") as "Sales Agent Name",
        "total_revenue",
        "discount_sum"
    from {{ ref('fct_sales') }}
),

agent_stats as (
    select
        "Sales Agent Name",
        count(distinct "Reference ID") as sales_count,
        avg("total_revenue") as avg_revenue,
        avg("discount_sum") as avg_discount,
        sum("total_revenue") as total_revenue
    from exploded_agents
    group by "Sales Agent Name"
)

select
    "Sales Agent Name",
    ROUND(sales_count::numeric, 2) as sales_count,
    ROUND(avg_revenue::numeric, 2) as avg_revenue, 
    ROUND(avg_discount::numeric, 2) as avg_discount,
    ROUND(total_revenue::numeric, 2) as total_revenue,
    rank() over (order by total_revenue desc) as rank
from agent_stats
order by rank;
