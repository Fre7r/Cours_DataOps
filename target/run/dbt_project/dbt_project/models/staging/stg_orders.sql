
  
  create view "warehouse"."main"."stg_orders__dbt_tmp" as (
    select * from staging.orders
  );
