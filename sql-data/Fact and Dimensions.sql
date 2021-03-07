-- DIM_PRODUCT_CATEGORY --
DROP TABLE IF EXISTS olist_db.d_product_category;
SET @rownr=0;
CREATE TABLE olist_db.d_product_category
SELECT @rownr:=@rownr+1 AS category_id, products.category_name AS category_name FROM (
SELECT DISTINCT product_category_name  AS category_name FROM olist_db.olist_products_dataset AS products_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id
) AS products;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product_category;
-- SELECT * FROM olist_db.d_product_category;

-- DIM_PRODUCT -- 
DROP TABLE IF EXISTS olist_db.d_product;
CREATE TABLE olist_db.d_product
SELECT DISTINCT products_dataset.product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty FROM olist_db.olist_products_dataset AS products_dataset
LEFT JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product;
-- SELECT * FROM olist_db.d_product;

-- DIM_LOCATION_STATE --
DROP TABLE IF EXISTS olist_db.d_state;
SET @rownr=0;
CREATE TABLE olist_db.d_state
SELECT @rownr:=@rownr+1 AS state_id, customers.state AS state FROM (SELECT DISTINCT(customer_state) AS state FROM olist_db.olist_customers_dataset) AS customers;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_state;
-- SELECT * FROM olist_db.d_state;

-- DIM_LOCATION_CITY --
DROP TABLE IF EXISTS olist_db.d_city;
SET @rownr=0;
CREATE TABLE olist_db.d_city
SELECT @rownr:=@rownr+1 AS city_id, state_id, customers.customer_city AS city FROM (SELECT DISTINCT customer_city, customer_state FROM olist_db.olist_customers_dataset) AS customers
INNER JOIN olist_db.d_state AS location_state ON location_state.state = customers.customer_state;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_city;
-- SELECT * FROM olist_db.d_city;
-- SELECT city, state FROM olist_db.d_city as location_city
-- INNER JOIN olist_db.d_state as location_state ON location_state.state_id = location_city.state_id
-- WHERE state = 'MG' order by city;

-- DIM_PAYMENT_TYPE --
DROP TABLE IF EXISTS olist_db.d_payment_type;
SET @rownr=0;
CREATE TABLE olist_db.d_payment_type
SELECT @rownr:=@rownr+1 AS type_id, payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_payment_type;
-- SELECT * FROM olist_db.d_payment_type;

-- DIM_PAYMENT  -- 
DROP TABLE IF EXISTS olist_db.d_payment;
SET @rownr=0;
CREATE TABLE olist_db.d_payment
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

-- DIM_TEMP_PAYMENT  -- 
DROP TABLE IF EXISTS olist_db.temp_payment;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.temp_payment
SELECT @rownr:=@rownr+1 AS payment_id, type_id, order_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type;
-- TEST
-- SELECT COUNT(*) FROM olist_db.temp_payment;

-- DIM_REVIEW --
DROP TABLE IF EXISTS olist_db.d_review;
CREATE TABLE olist_db.d_review
SELECT review_id, review_score FROM olist_db.olist_order_reviews_dataset;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_review;
-- SELECT * FROM olist_db.d_review;

DROP TABLE IF EXISTS olist_db.temp_review;
CREATE TEMPORARY TABLE olist_db.temp_review
SELECT review_id, order_id, review_score FROM olist_db.olist_order_reviews_dataset;

-- FACT_SALES_AMOUNT --
DROP TABLE IF EXISTS olist_db.f_sales;
SET @payment_id=0;
CREATE TABLE olist_db.f_sales
SELECT DISTINCT
product_id
, city_id
, payment_id
, review_id
FROM 
olist_db.olist_orders_dataset AS orders_dataset
LEFT JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.temp_payment AS temp_payment ON temp_payment.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_customers_dataset AS customers_dataset ON customers_dataset.customer_id = orders_dataset.customer_id
LEFT JOIN olist_db.d_city AS city ON city.city = customers_dataset.customer_city
LEFT JOIN olist_db.temp_review AS temp_review ON temp_review.order_id = orders_dataset.order_id;
-- TEST
-- SELECT * FROM olist_db.f_sales limit 100;
-- SELECT distinct payment_value FROM olist_db.f_sales AS sales 
-- INNER JOIN olist_db.d_payment AS payment ON payment.payment_id = sales.payment_id
-- WHERE order_id = '00337fe25a3780b3424d9ad7c5a4b35e'
-- UNION
-- select payment_value from  olist_db.olist_order_payments_dataset where order_id = '00337fe25a3780b3424d9ad7c5a4b35e';

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



