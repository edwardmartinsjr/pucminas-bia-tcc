-- Fact and Dimensions -- 

-- DIM_CUSTOMER ---
DROP TABLE IF EXISTS olist_db.d_customer;
CREATE TEMPORARY TABLE olist_db.d_customer
SELECT customer_id, customer_zip_code_prefix 
FROM olist_db.olist_customers_dataset;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_customer;
-- SELECT * FROM olist_db.d_customer;

-- DIM_SELLER -- 
DROP TABLE IF EXISTS olist_db.d_seller;
CREATE TEMPORARY TABLE olist_db.d_seller
SELECT seller_id, seller_zip_code_prefix 
FROM olist_db.olist_sellers_dataset;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_seller;
-- SELECT * FROM olist_db.d_seller;

-- DIM_PRODUCT_CATEGORY --
DROP TABLE IF EXISTS olist_db.d_product_category;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_product_category
SELECT @rownr:=@rownr+1 AS category_sk, products.category_name as category_name FROM (SELECT DISTINCT(product_category_name) AS category_name FROM olist_db.olist_products_dataset) AS products;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product_category;
-- SELECT * FROM olist_db.d_product_category;

-- DIM_PRODUCT -- 
DROP TABLE IF EXISTS olist_db.d_product;
CREATE TEMPORARY TABLE olist_db.d_product
SELECT product_id, category_sk, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm 
FROM olist_db.olist_products_dataset AS products_dataset
INNER JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_product;
-- SELECT * FROM olist_db.d_product;

-- DIM_LOCATION_CITY --
DROP TABLE IF EXISTS olist_db.d_location_city;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_location_city
SELECT @rownr:=@rownr+1 AS city_sk, geolocation.city as city FROM (SELECT DISTINCT(geolocation_city) AS city FROM olist_db.olist_geolocation_dataset) AS geolocation;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_location_city;
-- SELECT * FROM olist_db.d_location_city;

-- DIM_LOCATION_STATE --
DROP TABLE IF EXISTS olist_db.d_location_state;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_location_state
SELECT @rownr:=@rownr+1 AS state_sk, geolocation.state as state FROM (SELECT DISTINCT(geolocation_state) AS state FROM olist_db.olist_geolocation_dataset) AS geolocation;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_location_state;
-- SELECT * FROM olist_db.d_location_state;

-- DIM_LOCATION -- 
DROP TABLE IF EXISTS olist_db.d_location;
CREATE TEMPORARY TABLE olist_db.d_location
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, city_sk, state_sk 
FROM olist_db.olist_geolocation_dataset AS geolocation_dataset
INNER JOIN olist_db.d_location_city AS location_city ON geolocation_dataset.geolocation_city = location_city.city
INNER JOIN olist_db.d_location_state AS location_state ON geolocation_dataset.geolocation_state = location_state.state;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_location;
-- SELECT * FROM olist_db.d_location;

-- DIM_ITEM --
DROP TABLE IF EXISTS olist_db.d_item;
CREATE TEMPORARY TABLE olist_db.d_item
SELECT order_item_id, order_id,  product_id, seller_id, shipping_limit_date, price, freight_value FROM olist_db.olist_order_items_dataset ORDER BY order_id;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_item;
-- SELECT * FROM olist_db.d_item;

-- DIM_PAYMENT_TYPE --
DROP TABLE IF EXISTS olist_db.d_payment_type;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_payment_type
SELECT @rownr:=@rownr+1 AS type_sk, payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_payment_type;
-- SELECT * FROM olist_db.d_payment_type;

-- DIM_PAYMENT  -- 
DROP TABLE IF EXISTS olist_db.d_payment;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_payment
SELECT @rownr:=@rownr+1 AS payment_sk, order_id, payment_sequential, type_sk, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payments_dataset.payment_type = payment_type.payment_type;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_payment;
-- SELECT * FROM olist_db.d_payment;

-- DIM_REVIEW --
DROP TABLE IF EXISTS olist_db.d_review;
CREATE TEMPORARY TABLE olist_db.d_review
SELECT review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp FROM olist_db.olist_order_reviews_dataset;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_review;
-- SELECT * FROM olist_db.d_review;

-- DIM_ORDER_STATUS --
DROP TABLE IF EXISTS olist_db.d_order_status;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.d_order_status
SELECT @rownr:=@rownr+1 AS status_sk, orders.status as status FROM (SELECT DISTINCT(order_status) AS status FROM olist_db.olist_orders_dataset) AS orders;
-- TEST
-- SELECT COUNT(*) FROM olist_db.d_order_status;
-- SELECT * FROM olist_db.d_order_status;

-- FACT_SALES_AMOUNT --
DROP TABLE IF EXISTS olist_db.f_sales;
CREATE TEMPORARY TABLE olist_db.f_sales
SELECT 
orders_dataset.order_id
, customer.customer_id
, customer.customer_zip_code_prefix
, seller.seller_id
, seller.seller_zip_code_prefix
, item.order_item_id
, product.product_id
, product.category_sk
, order_status.status_sk
, payment.payment_sk
, review.review_id
, item.price
, order_purchase_timestamp
, order_approved_at
, order_delivered_carrier_date
, order_delivered_customer_date
, order_estimated_delivery_date
FROM olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.d_customer AS customer ON orders_dataset.customer_id = customer.customer_id
INNER JOIN olist_db.d_order_status AS order_status ON orders_dataset.order_status = order_status.status
INNER JOIN olist_db.d_item AS item ON orders_dataset.order_id = item.order_id
INNER JOIN olist_db.d_seller AS seller ON item.seller_id = seller.seller_id
INNER JOIN olist_db.d_product AS product ON item.product_id = product.product_id
INNER JOIN olist_db.d_payment AS payment ON orders_dataset.order_id = payment.order_id
INNER JOIN olist_db.d_review AS review ON orders_dataset.order_id = review.order_id;
-- TEST
-- SELECT COUNT(*) FROM olist_db.f_sales;
-- SELECT * FROM olist_db.f_sales where customer_id = 'b3ab3d70a498cea9282b87bc2fdf4c00';
-- SELECT customer_id, sum(price)  FROM olist_db.f_sales where customer_id = 'b3ab3d70a498cea9282b87bc2fdf4c00' group by customer_id ;
-- SELECT order_id, sum(price)  FROM olist_db.f_sales where customer_id = 'b3ab3d70a498cea9282b87bc2fdf4c00' group by order_id ;
-- SELECT order_id, sum(price)  FROM olist_db.f_sales group by order_id ;
-- SELECT customer_zip_code_prefix, sum(price)  FROM olist_db.f_sales group by customer_zip_code_prefix;

SELECT state, sum(price) as total_sales FROM olist_db.f_sales AS f_sales
INNER JOIN olist_db.d_location AS location ON f_sales.customer_zip_code_prefix = location.geolocation_zip_code_prefix
INNER JOIN olist_db.d_location_state AS location_state ON location.state_sk = location_state.state_sk
GROUP BY state
ORDER BY total_sales desc;

-- SELECT city, sum(price) as total_sales  FROM olist_db.f_sales AS f_sales
-- INNER JOIN olist_db.d_location AS location ON f_sales.customer_zip_code_prefix = location.geolocation_zip_code_prefix
-- INNER JOIN olist_db.d_location_city AS location_city ON location.city_sk = location_city.city_sk
-- GROUP BY city
-- ORDER BY total_sales desc;

-- b3ab3d70a498cea9282b87bc2fdf4c00	97.89
-- 17	b3ab3d70a498cea9282b87bc2fdf4c00	-23.56851813765943	-46.651915325696834	1	566f6035da0eb2a5d974ac66ae6d148c	21.99
-- 18	b3ab3d70a498cea9282b87bc2fdf4c00	-23.56851813765943	-46.651915325696834	1	3516632e8f52b679ff83d1665ecc990e	75.9

