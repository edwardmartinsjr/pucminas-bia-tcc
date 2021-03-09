-- DELETE -- 
DROP TABLE IF EXISTS olist_db.f_sales;
DROP TABLE IF EXISTS olist_db.d_review;
DROP TABLE IF EXISTS olist_db.d_product;
DROP TABLE IF EXISTS olist_db.d_product_category;
DROP TABLE IF EXISTS olist_db.d_payment;
DROP TABLE IF EXISTS olist_db.d_payment_type;
DROP TABLE IF EXISTS olist_db.d_city;
DROP TABLE IF EXISTS olist_db.d_state;

-- CREATE ---
CREATE TABLE `olist_db`.`d_state` (
  `state_id` bigint NOT NULL AUTO_INCREMENT,
  `state` text,
  PRIMARY KEY (`state_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_state` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_city` (
  `city_id` bigint NOT NULL AUTO_INCREMENT,
  `state_id` bigint DEFAULT NULL,
  `city` text,
  PRIMARY KEY (`city_id`),
  KEY `state_id_idx` (`state_id`),
  CONSTRAINT `state_id` FOREIGN KEY (`state_id`) REFERENCES `d_state` (`state_id`)
) ENGINE=InnoDB AUTO_INCREMENT=99164 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_city` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_payment_type` (
  `type_id` bigint NOT NULL AUTO_INCREMENT,
  `payment_type` text,
  PRIMARY KEY (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_payment_type` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_payment` (
  `payment_id` bigint NOT NULL AUTO_INCREMENT,
  `type_id` bigint DEFAULT NULL,
  `payment_sequential` int DEFAULT NULL,
  `payment_installments` int DEFAULT NULL,
  `payment_value` double DEFAULT NULL,
  PRIMARY KEY (`payment_id`),
  KEY `type_id_idx` (`type_id`),
  CONSTRAINT `type_id` FOREIGN KEY (`type_id`) REFERENCES `d_payment_type` (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=103600 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_payment` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_product_category` (
  `category_id` bigint NOT NULL AUTO_INCREMENT,
  `category_name` text,
  PRIMARY KEY (`category_id`)
) ENGINE=InnoDB AUTO_INCREMENT=75 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_product_category` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_product` (
  `product_id` varchar(50) NOT NULL,
  `category_id` bigint DEFAULT '0',
  `product_name_lenght` bigint DEFAULT NULL,
  `product_description_lenght` bigint DEFAULT NULL,
  `product_photos_qty` int DEFAULT NULL,
  PRIMARY KEY (`product_id`),
  KEY `category_id_idx` (`category_id`),
  CONSTRAINT `category_id` FOREIGN KEY (`category_id`) REFERENCES `d_product_category` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `olist_db`.`d_review` (
  `review_id` varchar(50) NOT NULL,
  `review_score` int DEFAULT NULL,
  PRIMARY KEY (`review_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `olist_db`.`f_sales` (
  `product_id` varchar(50) DEFAULT NULL,
  `city_id` bigint DEFAULT NULL,
  `payment_id` bigint DEFAULT NULL,
  `review_id` varchar(50),
  KEY `product_id_idx` (`product_id`),
  KEY `city_id_idx` (`city_id`),
  KEY `payment_id_idx` (`payment_id`),
  KEY `review_id_idx` (`review_id`),
  CONSTRAINT `city_id` FOREIGN KEY (`city_id`) REFERENCES `d_city` (`city_id`),
  CONSTRAINT `payment_id` FOREIGN KEY (`payment_id`) REFERENCES `d_payment` (`payment_id`),
  CONSTRAINT `product_id` FOREIGN KEY (`product_id`) REFERENCES `d_product` (`product_id`),
  CONSTRAINT `review_id` FOREIGN KEY (`review_id`) REFERENCES `d_review` (`review_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- INSERT --
INSERT INTO olist_db.d_state (state) 
SELECT customers.state AS state FROM (SELECT DISTINCT(customer_state) AS state FROM olist_db.olist_customers_dataset) AS customers;

INSERT INTO olist_db.d_city (city, state_id)
(SELECT customers.customer_city AS city, state_id FROM (SELECT DISTINCT olist_customers_dataset.customer_id, customer_city, customer_state FROM olist_db.olist_customers_dataset AS olist_customers_dataset
INNER JOIN olist_db.olist_orders_dataset AS olist_orders_datase ON olist_orders_datase.customer_id = olist_customers_dataset.customer_id) AS customers
INNER JOIN olist_db.d_state AS state ON state.state = customers.customer_state);

INSERT INTO olist_db.d_payment_type (payment_type)
(SELECT payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments);

INSERT INTO olist_db.d_payment (type_id, payment_sequential, payment_installments, payment_value)
(SELECT type_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type);

INSERT INTO olist_db.d_product_category (category_name)
(SELECT DISTINCT product_category_name  AS category_name FROM olist_db.olist_products_dataset AS products_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);

INSERT INTO olist_db.d_product (product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty)
(SELECT DISTINCT products_dataset.product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty FROM olist_db.olist_products_dataset AS products_dataset
LEFT JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);

INSERT INTO olist_db.d_review (review_id, review_score)
(SELECT review_id, review_score FROM olist_db.olist_order_reviews_dataset);

-- TEMPORARY TABLES --
DROP TABLE IF EXISTS olist_db.temp_city;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.temp_city
SELECT @rownr:=@rownr+1 AS city_id, state_id, customers.customer_city AS city, customers.customer_id FROM (SELECT DISTINCT olist_customers_dataset.customer_id, customer_city, customer_state FROM olist_db.olist_customers_dataset AS olist_customers_dataset
INNER JOIN olist_db.olist_orders_dataset AS olist_orders_datase ON olist_orders_datase.customer_id = olist_customers_dataset.customer_id) AS customers
INNER JOIN olist_db.d_state AS location_state ON location_state.state = customers.customer_state;

DROP TABLE IF EXISTS olist_db.temp_payment;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.temp_payment
SELECT @rownr:=@rownr+1 AS payment_id, type_id, order_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type;

DROP TABLE IF EXISTS olist_db.temp_review;
CREATE TEMPORARY TABLE olist_db.temp_review
SELECT review_id, order_id, review_score FROM olist_db.olist_order_reviews_dataset;

-- FACT_SALES --
INSERT INTO olist_db.f_sales (product_id, city_id, payment_id, review_id)
(SELECT DISTINCT
product_id
, city_id
, payment_id
, review_id
FROM 
olist_db.olist_orders_dataset AS orders_dataset
LEFT JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.temp_payment AS temp_payment ON temp_payment.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_customers_dataset AS customers_dataset ON customers_dataset.customer_id = orders_dataset.customer_id
LEFT JOIN olist_db.temp_city AS temp_city ON temp_city.customer_id = customers_dataset.customer_id
LEFT JOIN olist_db.temp_review AS temp_review ON temp_review.order_id = orders_dataset.order_id);