{{ config(materialized='table') }}

with base as (
    select
        COALESCE("Reference ID", 'N/A') as "Reference ID",
        COALESCE("Product Name", 'N/A') as "Product Name",
        COALESCE("Sales Agent Name", 'N/A') as "Sales Agent Name",
        COALESCE("Country", 'N/A') as "Country",
        COALESCE("Campaign Name", 'N/A') as "Campaign Name",
        COALESCE("Source", 'N/A') as "Source",
        COALESCE(
            COALESCE("Total Amount ($)", 0) + 
            COALESCE("Total Rebill Amount", 0) - 
            COALESCE("Returned Amount ($)", 0), 0) as Total_Revenue,
        COALESCE("Total Rebill Amount", 0) as Rebill_Revenue,
        COALESCE("Number Of Rebills", 0) as Rebill_Count,
        COALESCE("Discount Amount ($)", 0) as Discount_Sum,
        COALESCE("Returned Amount ($)", 0) as Returned_Sum,
        "Order Date Kyiv",
        "Return Date Kyiv",
        "Return Date Kyiv"::timestamp at time zone 'Europe/Kiev' at time zone 'UTC' as Return_Date_UTC,
        "Return Date Kyiv"::timestamp at time zone 'Europe/Kiev' at time zone 'America/New_York' as Return_Date_NY,
        "Order Date Kyiv"::timestamp at time zone 'Europe/Kiev' at time zone 'UTC' as Order_Date_UTC,
        "Order Date Kyiv"::timestamp at time zone 'Europe/Kiev' at time zone 'America/New_York' as Order_Date_NY,
        extract(day from ("Return Date Kyiv"::timestamp - "Order Date Kyiv"::timestamp)) as Days_To_Return
    from {{ ref('sample_data_for_GYB_test') }} 
),

agg as (
    select
        "Reference ID",
        "Product Name",
        array_agg(distinct "Sales Agent Name") as Sales_Agent_List,
        "Country",
        "Campaign Name",
        "Source",
        sum(Total_Revenue) as Total_Revenue,
        sum(Rebill_Revenue) as Rebill_Revenue,
        sum(Rebill_Count) as Rebill_Count,
        sum(Discount_Sum) as Discount_Sum,
        sum(Returned_Sum) as Returned_Sum,
        max("Order Date Kyiv") as Order_Date_Kyiv,
        max(Order_Date_UTC) as Order_Date_UTC,
        max(Order_Date_NY) as Order_Date_NY,
        max("Return Date Kyiv") as Return_Date_Kyiv,
        max(Return_Date_UTC) as Return_Date_UTC,
        max(Return_Date_NY) as Return_Date_NY,
        max(Days_To_Return) as Days_To_Return
    from base
    group by 
        "Reference ID",
        "Product Name",
        "Country",
        "Campaign Name",
        "Source"
)

select * from agg