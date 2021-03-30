-- DELETE -- 
DROP TABLE IF EXISTS olist_db.f_sales;
DROP TABLE IF EXISTS olist_db.d_order;
DROP TABLE IF EXISTS olist_db.d_review;
DROP TABLE IF EXISTS olist_db.d_product;
DROP TABLE IF EXISTS olist_db.d_product_category;
DROP TABLE IF EXISTS olist_db.d_payment;
DROP TABLE IF EXISTS olist_db.d_payment_type;
DROP TABLE IF EXISTS olist_db.d_city;
DROP TABLE IF EXISTS olist_db.d_state;
DROP TABLE IF EXISTS olist_db.d_hour;
DROP TABLE IF EXISTS olist_db.d_day;
DROP TABLE IF EXISTS olist_db.d_month;
DROP TABLE IF EXISTS olist_db.d_year;

-- CREATE ---
CREATE TABLE `olist_db`.`d_state` (
  `state_id` bigint NOT NULL AUTO_INCREMENT,
  `state` text,
  PRIMARY KEY (`state_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_city` (
  `city_id` bigint NOT NULL AUTO_INCREMENT,
  `state_id` bigint NOT NULL,
  `city` text,
  PRIMARY KEY (`city_id`),
  KEY `state_id_idx` (`state_id`),
  CONSTRAINT `state_id` FOREIGN KEY (`state_id`) REFERENCES `d_state` (`state_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_payment_type` (
  `type_id` bigint NOT NULL AUTO_INCREMENT,
  `payment_type` text,
  PRIMARY KEY (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_payment` (
  `payment_id` bigint NOT NULL AUTO_INCREMENT,
  `type_id` bigint NOT NULL,
  `payment_sequential` int NOT NULL,
  `payment_installments` int NOT NULL,
  `payment_value` double NOT NULL,
  PRIMARY KEY (`payment_id`),
  KEY `type_id_idx` (`type_id`),
  CONSTRAINT `type_id` FOREIGN KEY (`type_id`) REFERENCES `d_payment_type` (`type_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_product_category` (
  `category_id` bigint NOT NULL AUTO_INCREMENT,
  `category_name` text,
  PRIMARY KEY (`category_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_product` (
  `product_id` varchar(50) NOT NULL,
  `category_id` bigint DEFAULT '0',
  `product_name_lenght` bigint DEFAULT NULL,
  `product_description_lenght` bigint DEFAULT NULL,
  `product_photos_qty` int DEFAULT NULL,
  PRIMARY KEY (`product_id`),
  KEY `category_id_idx` (`category_id`),
  CONSTRAINT `category_id` FOREIGN KEY (`category_id`) REFERENCES `d_product_category` (`category_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_review` (
  `review_id` varchar(50) NOT NULL,
  `review_score` int NOT NULL,
  PRIMARY KEY (`review_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`d_hour` (
  `hour_id` bigint NOT NULL AUTO_INCREMENT,
  `hour` int NOT NULL,
  PRIMARY KEY (`hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
ALTER TABLE `olist_db`.`d_hour` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_day` (
  `day_id` bigint NOT NULL AUTO_INCREMENT,
  `day` int NOT NULL,
  PRIMARY KEY (`day_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
ALTER TABLE `olist_db`.`d_day` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_month` (
  `month_id` bigint NOT NULL AUTO_INCREMENT,
  `month` int NOT NULL,
  PRIMARY KEY (`month_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
ALTER TABLE `olist_db`.`d_month` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_year` (
  `year_id` bigint NOT NULL AUTO_INCREMENT,
  `year` int NOT NULL,
  PRIMARY KEY (`year_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
ALTER TABLE `olist_db`.`d_year` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_order` (
  `order_id` varchar(50) NOT NULL,
  `order_status` text,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`f_sales` (
  `order_id` varchar(50) NOT NULL,
  `product_id` varchar(50) NOT NULL,
  `city_id` bigint NOT NULL,
  `payment_id` bigint NOT NULL,
  `review_id` varchar(50),
  `hour_id` bigint NOT NULL,
  `day_id` bigint NOT NULL,
  `month_id` bigint NOT NULL,
  `year_id` bigint NOT NULL,
  price double NOT NULL,
  KEY `order_id_idx` (`order_id`),
  KEY `product_id_idx` (`product_id`),
  KEY `city_id_idx` (`city_id`),
  KEY `payment_id_idx` (`payment_id`),
  KEY `review_id_idx` (`review_id`),
  KEY `hour_id_idx` (`hour_id`),
  KEY `day_id_idx` (`day_id`),
  KEY `month_id_idx` (`month_id`),
  KEY `year_id_idx` (`year_id`),
  CONSTRAINT `order_id` FOREIGN KEY (`order_id`) REFERENCES `d_order` (`order_id`),
  CONSTRAINT `city_id` FOREIGN KEY (`city_id`) REFERENCES `d_city` (`city_id`),
  CONSTRAINT `payment_id` FOREIGN KEY (`payment_id`) REFERENCES `d_payment` (`payment_id`),
  CONSTRAINT `product_id` FOREIGN KEY (`product_id`) REFERENCES `d_product` (`product_id`),
  CONSTRAINT `review_id` FOREIGN KEY (`review_id`) REFERENCES `d_review` (`review_id`),
  CONSTRAINT `hour_id` FOREIGN KEY (`hour_id`) REFERENCES `d_hour` (`hour_id`),
  CONSTRAINT `day_id` FOREIGN KEY (`day_id`) REFERENCES `d_day` (`day_id`),
  CONSTRAINT `month_id` FOREIGN KEY (`month_id`) REFERENCES `d_month` (`month_id`),
  CONSTRAINT `year_id` FOREIGN KEY (`year_id`) REFERENCES `d_year` (`year_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE `olist_db`.`d_state` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_city` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_payment_type` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_payment` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_product_category` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_hour` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_day` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_month` AUTO_INCREMENT=1;
ALTER TABLE `olist_db`.`d_year` AUTO_INCREMENT=1;

-- INSERT --
INSERT INTO olist_db.d_state (state) 
SELECT customers.state AS state FROM (SELECT DISTINCT(customer_state) AS state FROM olist_db.olist_customers_dataset) AS customers;

INSERT INTO olist_db.d_city (city, state_id)
(SELECT customers.customer_city AS city, state_id FROM (SELECT DISTINCT customer_city, customer_state FROM olist_db.olist_customers_dataset AS olist_customers_dataset
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

INSERT INTO olist_db.d_order (order_id, order_status)
(SELECT order_id, order_status FROM olist_db.olist_orders_dataset);

INSERT INTO olist_db.d_hour (`hour`)
SELECT DISTINCT HOUR(order_approved_at) AS `hour` FROM olist_db.olist_orders_dataset
WHERE order_approved_at IS NOT NULL;

INSERT INTO olist_db.d_day (`day`)
SELECT DISTINCT DAY(order_approved_at) AS `day` FROM olist_db.olist_orders_dataset
WHERE order_approved_at IS NOT NULL;

INSERT INTO olist_db.d_month (`month`)
SELECT DISTINCT MONTH(order_approved_at) AS `month` FROM olist_db.olist_orders_dataset
WHERE order_approved_at IS NOT NULL;

INSERT INTO olist_db.d_year (`year`)
SELECT DISTINCT YEAR(order_approved_at) AS `year` FROM olist_db.olist_orders_dataset
WHERE order_approved_at IS NOT NULL;

-- TEMPORARY TABLES --
DROP TABLE IF EXISTS olist_db.temp_city;
CREATE TEMPORARY TABLE olist_db.temp_city
SELECT 
location.city_id,
location.state_id,
location.city,
location.state,
customers_dataset.customer_id
FROM
(SELECT city_id, location_state.state_id, city, state FROM olist_db.d_city AS location_city
INNER JOIN olist_db.d_state AS location_state ON location_city.state_id = location_state.state_id) AS location,
(SELECT customers_dataset.customer_id, customer_city AS city, customer_state AS state FROM olist_db.olist_customers_dataset AS customers_dataset
INNER JOIN olist_db.olist_orders_dataset AS orders_datase ON orders_datase.customer_id = customers_dataset.customer_id) AS customers_dataset
WHERE customers_dataset.city = location.city
AND customers_dataset.state = location.state;

DROP TABLE IF EXISTS olist_db.temp_payment;
SET @rownr=0;
CREATE TEMPORARY TABLE olist_db.temp_payment
SELECT @rownr:=@rownr+1 AS payment_id, type_id, order_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type;

-- FACT_SALES --
INSERT INTO olist_db.f_sales (order_id, product_id, city_id, payment_id, review_id, hour_id, day_id, month_id, year_id, price)
(SELECT 
orders_dataset.order_id
, product_id
, city_id
, payment_id
, review_id
, (SELECT hour_id FROM olist_db.d_hour WHERE hour = HOUR(order_approved_at)) AS hour_id
, (SELECT day_id FROM olist_db.d_day WHERE day = DAY(order_approved_at)) AS day_id
, (SELECT month_id FROM olist_db.d_month WHERE month = MONTH(order_approved_at)) AS month_id
, (SELECT year_id FROM olist_db.d_year WHERE year = YEAR(order_approved_at)) AS year_id
, order_items_dataset.price
FROM 
olist_db.olist_orders_dataset AS orders_dataset
INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.order_id = orders_dataset.order_id
INNER JOIN olist_db.temp_payment AS temp_payment ON temp_payment.order_id = orders_dataset.order_id
INNER JOIN olist_db.olist_customers_dataset AS customers_dataset ON customers_dataset.customer_id = orders_dataset.customer_id
INNER JOIN olist_db.temp_city AS temp_city ON temp_city.customer_id = customers_dataset.customer_id
INNER JOIN olist_db.olist_order_reviews_dataset AS olist_order_reviews_dataset ON olist_order_reviews_dataset.order_id = orders_dataset.order_id
WHERE order_approved_at IS NOT NULL);