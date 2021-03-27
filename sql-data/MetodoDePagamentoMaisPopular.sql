SELECT payment_type as `Payment type`, sum(price) as Price FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
INNER JOIN olist_db.d_payment AS payment ON payment.payment_id = sales.payment_id
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.type_id = payment.type_id
group by payment_type
Order by `Payment type` desc;