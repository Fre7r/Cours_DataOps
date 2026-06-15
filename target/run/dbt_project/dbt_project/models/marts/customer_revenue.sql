
  
    
    

    create  table
      "warehouse"."main"."customer_revenue__dbt_tmp"
  
    as (
      with valid_items as (
    select * from "warehouse"."main"."int_valid_order_items"
),

customers as (
    select * from "warehouse"."main"."stg_customers"
),

revenue_per_customer as (
    select
        customer_id,
        count(distinct order_id) as nb_orders,
        sum(revenue)             as total_revenue
    from valid_items
    group by customer_id
)

select
    c.customer_id,
    c.country,
    c.segment,
    rpc.nb_orders,
    round(rpc.total_revenue, 2) as total_revenue
from revenue_per_customer rpc
join customers c on rpc.customer_id = c.customer_id
order by total_revenue desc
    );
  
  