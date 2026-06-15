
  
  create view "warehouse"."main"."stg_customers__dbt_tmp" as (
    select * from staging.customers
  );
