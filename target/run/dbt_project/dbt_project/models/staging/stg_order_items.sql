
  
  create view "warehouse"."main"."stg_order_items__dbt_tmp" as (
    select * from staging.order_items
  );
