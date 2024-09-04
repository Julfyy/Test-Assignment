with overall_avg_discount as (
    select
        avg("discount_sum") as overall_avg_discount
    from {{ ref('fct_sales') }}
),

agent_avg_discounts as (
    select
        unnest("sales_agent_list") as "Sales Agent Name",
        avg("discount_sum") as agent_avg_discount
    from {{ ref('fct_sales') }}
    group by "Sales Agent Name"
)

select
    "Sales Agent Name",
    ROUND(agent_avg_discount::numeric, 2)
from agent_avg_discounts
where agent_avg_discount > (select overall_avg_discount from overall_avg_discount)
order by agent_avg_discount desc;
