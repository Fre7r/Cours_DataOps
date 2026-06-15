with valid_items as (
    select * from {{ ref('int_valid_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    p.category,
    count(*)                   as nb_line_items,
    sum(vi.quantity)           as total_units_sold,
    round(sum(vi.revenue), 2)  as total_revenue
from valid_items vi
join products p on vi.product_id = p.product_id
group by p.category
order by total_revenue desc