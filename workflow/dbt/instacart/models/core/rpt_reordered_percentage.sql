WITH cte_ratio as (
    SELECT
      product_id,
      round(AVG(reordered) * 100, 2) AS `reordered_percentage`
    FROM
       {{ source('core','fact_order_product') }} 
    GROUP BY
		product_id
), cte_product as (
  SELECT
      product_id, 
      product_name,
      SUM(num_order) AS `num_of_order`
    FROM
	   {{ ref('rpt_order_product') }}
    GROUP BY product_id, product_name
) select cte_ratio.product_id, reordered_percentage, product_name, num_of_order
    from cte_ratio inner join cte_product on cte_ratio.product_id = cte_product.product_id