SELECT count(distinct orders_dataset.order_id)
FROM 
olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.temp_payment AS temp_payment ON temp_payment.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_customers_dataset AS customers_dataset ON customers_dataset.customer_id = orders_dataset.customer_id
INNER JOIN olist_db.temp_city AS temp_city ON temp_city.customer_id = customers_dataset.customer_id
LEFT JOIN olist_db.temp_review AS temp_review ON temp_review.order_id = orders_dataset.order_id
LEFT JOIN olist_db.temp_hour AS temp_hour ON temp_hour.order_id = orders_dataset.order_id
LEFT JOIN olist_db.temp_day AS temp_day ON temp_day.order_id = orders_dataset.order_id
LEFT JOIN olist_db.temp_month AS temp_month ON temp_month.order_id = orders_dataset.order_id
LEFT JOIN olist_db.temp_year AS temp_year ON temp_year.order_id = orders_dataset.order_id
WHERE order_approved_at IS NOT NULL
UNION
SELECT count(distinct orders_dataset.order_id) 
FROM 
olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
WHERE order_approved_at IS NOT NULL;

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
AND customer_state = 'MG';

SELECT distinct sales.product_id, city, state FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
WHERE 
state = 'MG' AND 
sales.product_id = '2bd9b51a9ab079e095aca987845d3266'
UNION
SELECT distinct products_dataset.product_id, customer_city, customer_state FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
WHERE 
customer_state = 'MG' AND 
products_dataset.product_id = '2bd9b51a9ab079e095aca987845d3266';

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
UNION
SELECT count(distinct products_dataset.product_id, customer_city, customer_state) FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
INNER JOIN olist_db.olist_order_payments_dataset as order_payments_dataset on order_payments_dataset.order_id = orders_dataset.order_id
WHERE product_category_name = 'consoles_games'
AND customer_state = 'MG'
AND payment_type = 'boleto';

SELECT count(distinct order_id) FROM olist_db.f_sales as sales
UNION
SELECT count(distinct orders_dataset.order_id) FROM olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
WHERE order_approved_at IS NOT NULL;

SELECT count(distinct product_id) FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_day as d_day on d_day.day_id = sales.day_id
where `day` = 1
UNION
SELECT count(distinct products_dataset.product_id) FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
where day(order_approved_at) = 1;

SELECT  product_id, `year`, `month`, `day`, `hour`, price  FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_hour as d_hour on d_hour.hour_id = sales.hour_id
INNER JOIN olist_db.d_day as d_day on d_day.day_id = sales.day_id
INNER JOIN olist_db.d_month as d_month on d_month.month_id = sales.month_id
INNER JOIN olist_db.d_year as d_year on d_year.year_id = sales.year_id
where sales.product_id = 'df3655ac9aa8c6cbfa63bdd8d3b09c50'
order by `year`, `month`, `day`, `hour`;

SELECT products_dataset.product_id, order_approved_at, order_items_dataset.price FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
where products_dataset.product_id = 'df3655ac9aa8c6cbfa63bdd8d3b09c50'
order by order_approved_at;

SELECT sum(price) FROM (
	SELECT max(price) as price FROM olist_db.f_sales
    group by order_id
) as sales
UNION
SELECT sum(price) from 
(SELECT max(price) as price FROM olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
WHERE order_approved_at IS NOT NULL
group by orders_dataset.order_id) as orders;

SELECT sum(price) FROM (
	SELECT max(price) as price FROM olist_db.f_sales as sales
    INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
	INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
    AND state = 'MG'
    group by order_id
) as sales
UNION
SELECT sum(price) from 
(SELECT max(price) as price FROM olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
WHERE order_approved_at IS NOT NULL
AND customer_state = 'MG'
group by orders_dataset.order_id) as orders;

SELECT sum(price) FROM olist_db.f_sales as sales
INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
WHERE 1=1
AND category_name = 'consoles_games'
AND state = 'MG'
UNION
SELECT sum(price) FROM olist_db.olist_orders_dataset as orders_dataset
INNER JOIN olist_db.olist_order_items_dataset as order_items_dataset on order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset as products_dataset on products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset as customers_dataset on customers_dataset.customer_id = orders_dataset.customer_id
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
WHERE 1=1
AND product_category_name = 'consoles_games'
AND customer_state = 'MG';


SET GLOBAL interactive_timeout=60;
SET GLOBAL connect_timeout=60;
select count(*) from olist_db.olist_customers_dataset
UNION
select count(*) from olist_db.olist_geolocation_dataset
UNION
select count(*) from olist_db.olist_order_items_dataset
UNION
select count(*) from olist_db.olist_order_payments_dataset
UNION
select count(*) from olist_db.olist_order_reviews_dataset
UNION
select count(*) from olist_db.olist_orders_dataset
UNION
select count(*) from olist_db.olist_products_dataset
UNION
select count(*) from olist_db.olist_sellers_dataset
UNION
select count(*) from olist_db.product_category_name_translation
UNION
select count(*) from olist_db.f_sales
UNION
select count(*) from olist_db.d_order
UNION
select count(*) from olist_db.d_review
UNION
select count(*) from olist_db.d_product
UNION
select count(*) from olist_db.d_product_category
UNION
select count(*) from olist_db.d_payment
UNION
select count(*) from olist_db.d_payment_type
UNION
select count(*) from olist_db.d_city
UNION
select count(*) from olist_db.d_state
UNION
select count(*) from olist_db.d_hour
UNION
select count(*) from olist_db.d_day
UNION
select count(*) from olist_db.d_month
UNION
select count(*) from olist_db.d_year; 