SELECT state, sum(price) as Price FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
WHERE 1=1
Group By state
Order by Price desc
limit 5