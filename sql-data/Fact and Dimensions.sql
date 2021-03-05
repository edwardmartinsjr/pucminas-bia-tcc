-- DIM_PRODUCT_CATEGORY --
DROP TABLE IF EXISTS olist_db.d_product_category;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_product_category
SELECT @rownr:=@rownr+1 AS category_id, products.category_name AS category_name FROM (SELECT DISTINCT(product_category_name) AS category_name FROM olist_db.olist_products_dataset) AS products;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product_category;
-- SELECT * FROM olist_db.d_product_category;

-- DIM_PRODUCT -- 
DROP TABLE IF EXISTS olist_db.d_product;
CREATE TEMPORARY TABLE olist_db.d_product
SELECT product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty
FROM olist_db.olist_products_dataset AS products_dataset
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product;
-- SELECT * FROM olist_db.d_product;

-- DIM_LOCATION_STATE --
DROP TABLE IF EXISTS olist_db.d_state;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_state
SELECT @rownr:=@rownr+1 AS state_id, geolocation.state AS state FROM (SELECT DISTINCT(geolocation_state) AS state FROM olist_db.olist_geolocation_dataset) AS geolocation;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_state;
-- SELECT * FROM olist_db.d_state;

-- DIM_LOCATION_CITY --
DROP TABLE IF EXISTS olist_db.d_city;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_city
SELECT @rownr:=@rownr+1 AS city_id, state_id, geolocation.geolocation_city AS city FROM (SELECT DISTINCT geolocation_city, geolocation_state FROM olist_db.olist_geolocation_dataset) AS geolocation
INNER JOIN olist_db.d_state AS location_state ON location_state.state = geolocation.geolocation_state;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_city;
-- SELECT * FROM olist_db.d_city;
-- SELECT city, state FROM olist_db.d_city as location_city
-- INNER JOIN olist_db.d_state as location_state ON location_state.state_id = location_city.state_id
-- WHERE state = 'MG' order by city;

-- DIM_PAYMENT_TYPE --
DROP TABLE IF EXISTS olist_db.d_payment_type;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_payment_type
SELECT @rownr:=@rownr+1 AS type_id, payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_payment_type;
-- SELECT * FROM olist_db.d_payment_type;

-- DIM_PAYMENT  -- 
DROP TABLE IF EXISTS olist_db.d_payment;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_payment
SELECT @rownr:=@rownr+1 AS payment_id, type_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_payment;
-- SELECT * FROM olist_db.d_payment;
-- SELECT sum(payment_value) FROM olist_db.d_payment as payment
-- INNER JOIN olist_db.d_payment_type as payment_type on payment_type.type_id = payment.type_id
-- where payment_type = 'credit_card'
-- UNION
-- select sum(payment_value) from olist_db.olist_order_payments_dataset where payment_type = 'credit_card';

-- DIM_REVIEW --
DROP TABLE IF EXISTS olist_db.d_review;
CREATE TEMPORARY TABLE olist_db.d_review
SELECT review_id, review_score FROM olist_db.olist_order_reviews_dataset;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_review;
-- SELECT * FROM olist_db.d_review;

-- FACT_SALES_AMOUNT --
DROP TABLE IF EXISTS olist_db.f_sales;
CREATE TEMPORARY TABLE olist_db.f_sales
SELECT 
products_dataset.product_id
, city_id
-- , payment_id
FROM 
olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_products_dataset AS products_dataset ON products_dataset.product_id = order_items_dataset.product_id
INNER JOIN olist_db.olist_customers_dataset AS customers_dataset ON customers_dataset.customer_id = orders_dataset.customer_id
LEFT JOIN olist_db.d_city AS city ON city.city = customers_dataset.customer_city
INNER JOIN olist_db.olist_order_payments_dataset AS order_payments_dataset ON order_payments_dataset.order_id = orders_dataset.order_id
-- LEFT JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = order_payments_dataset.payment_type
-- RIGHT JOIN olist_db.d_payment AS payment ON payment.type_id = payment_type.type_id
-- TEST
-- SELECT * FROM olist_db.f_sales limit 10;
-- SELECT count(*) FROM olist_db.f_sales as sales
-- INNER JOIN olist_db.d_product AS product ON product.product_id = sales.product_id
-- INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_id = product.category_id
-- INNER JOIN olist_db.d_city AS city ON city.city_id = sales.city_id
-- INNER JOIN olist_db.d_state AS state ON state.state_id = city.state_id
-- WHERE category_name = 'consoles_games'
-- AND state = 'MG';



