
SELECT * FROM olist_db.f_sales limit 100;
SELECT distinct payment_value FROM olist_db.f_sales AS sales 
INNER JOIN olist_db.d_payment AS payment ON payment.payment_id = sales.payment_id
WHERE order_id = '00337fe25a3780b3424d9ad7c5a4b35e'
UNION
select payment_value from  olist_db.olist_order_payments_dataset where order_id = '00337fe25a3780b3424d9ad7c5a4b35e';

SELECT count(distinct sales.product_id) FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
WHERE category_name = 'consoles_games'
AND state = 'MG'
UNION
SELECT count(distinct products_dataset.product_id) FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
WHERE product_category_name = 'consoles_games'
AND customer_state = 'MG'

SELECT count(distinct sales.product_id, city, state) FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
INNER JOIN olist_db.d_payment AS payment ON payment.payment_id = sales.payment_id
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.type_id = payment.type_id
WHERE category_name = 'consoles_games'
AND state = 'MG'
AND payment_type = 'boleto'
-- AND sales.product_id = '2bd9b51a9ab079e095aca987845d3266'
UNION
SELECT count(distinct products_dataset.product_id, customer_city, customer_state) FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
INNER JOIN olist_db.olist_order_payments_dataset as order_payments_dataset on order_payments_dataset.order_id = orders_dataset.order_id
WHERE product_category_name = 'consoles_games'
AND customer_state = 'MG'
AND payment_type = 'boleto'
-- AND products_dataset.product_id = '2bd9b51a9ab079e095aca987845d3266'