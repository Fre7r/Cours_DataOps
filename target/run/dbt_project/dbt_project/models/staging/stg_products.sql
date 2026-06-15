
  
  create view "warehouse"."main"."stg_products__dbt_tmp" as (
    select * from staging.products
  );
