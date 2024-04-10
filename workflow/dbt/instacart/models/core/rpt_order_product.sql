select 
    op.product_id, 
    p.product_name, 
    a.aisle, 
    a.aisle_id,
    d.department, 
    d.department_id,
    o.user_id,
    count(distinct(op.order_id)) num_order 
  from {{ source('core','fact_order_product') }} op
    inner join {{ source('core','fact_order') }} o on op.order_id = o.order_id
    inner join  {{ source('core','dim_product') }} p on op.product_id = p.product_id
    inner join {{ source('core','dim_aisle') }} a on p.aisle_id = a.aisle_id
    inner join {{ source('core','dim_department') }} d on d.department_id = p.department_id
  group by 
    op.product_id, 
    p.product_name, 
    a.aisle, 
    a.aisle_id, 
    d.department, 
    d.department_id, 
    o.user_id
