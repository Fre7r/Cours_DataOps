
  
  create view "warehouse"."main"."int_valid_order_items__dbt_tmp" as (
    with order_items as (
    select * from "warehouse"."main"."stg_order_items"
),

orders as (
    select * from "warehouse"."main"."stg_orders"
),

valid_orders as (
    select *
    from orders
    where status != 'cancelled'
)

select
    oi.item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    cast(oi.quantity as integer)                                  as quantity,
    cast(oi.quantity as integer) * cast(oi.unit_price as double)  as revenue
from order_items oi
join valid_orders o on oi.order_id = o.order_id
  );
