DROP DATABASE IF EXISTS `olist_db`;
CREATE DATABASE `olist_db`;

CREATE TABLE `olist_db`.`product_category_name_translation` (
  `product_category_name` text,
  `product_category_name_english` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_geolocation_dataset` (
  `geolocation_zip_code_prefix` VARCHAR(5),
  `geolocation_lat` double DEFAULT NULL,
  `geolocation_lng` double DEFAULT NULL,
  `geolocation_city` text,
  `geolocation_state` text,
  PRIMARY KEY (geolocation_zip_code_prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_products_dataset` (
  `product_id` VARCHAR(50),
  `product_category_name` text,
  `product_name_lenght` bigint DEFAULT NULL,
  `product_description_lenght` bigint DEFAULT NULL,
  `product_photos_qty` int DEFAULT NULL,
  `product_weight_g` bigint DEFAULT NULL,
  `product_length_cm` bigint DEFAULT NULL,
  `product_height_cm` bigint DEFAULT NULL,
  `product_width_cm` bigint DEFAULT NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_sellers_dataset` (
  `seller_id` VARCHAR(50),
  `seller_zip_code_prefix` VARCHAR(5),
  `seller_city` text,
  `seller_state` text,
  PRIMARY KEY (seller_id),
  FOREIGN KEY (seller_zip_code_prefix)
        REFERENCES olist_db.olist_geolocation_dataset(geolocation_zip_code_prefix)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_customers_dataset` (
  `customer_id` VARCHAR(50),
  `customer_unique_id` VARCHAR(50),
  `customer_zip_code_prefix` VARCHAR(5),
  `customer_city` text,
  `customer_state` text,
  PRIMARY KEY (customer_id),
  FOREIGN KEY (customer_zip_code_prefix)
        REFERENCES olist_db.olist_geolocation_dataset(geolocation_zip_code_prefix)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_orders_dataset` (
  `order_id` VARCHAR(50),
  `customer_id` VARCHAR(50),
  `order_status` text,
  `order_purchase_timestamp` datetime DEFAULT NULL,
  `order_approved_at` datetime DEFAULT NULL,
  `order_delivered_carrier_date` datetime DEFAULT NULL,
  `order_delivered_customer_date` datetime DEFAULT NULL,
  `order_estimated_delivery_date` datetime DEFAULT NULL,
  PRIMARY KEY (order_id),
  FOREIGN KEY (customer_id)
        REFERENCES olist_db.olist_customers_dataset(customer_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_order_items_dataset` (
  `order_id` VARCHAR(50),
  `order_item_id` VARCHAR(50),
  `product_id` VARCHAR(50),
  `seller_id` VARCHAR(50),
  `shipping_limit_date` datetime DEFAULT NULL,
  `price` double DEFAULT NULL,
  `freight_value` double DEFAULT NULL,
  FOREIGN KEY (order_id)
        REFERENCES olist_db.olist_orders_dataset(order_id)
        ON DELETE CASCADE,
  FOREIGN KEY (product_id)
        REFERENCES olist_db.olist_products_dataset(product_id)
        ON DELETE CASCADE,
  FOREIGN KEY (seller_id)
        REFERENCES olist_db.olist_sellers_dataset(seller_id)
        ON DELETE CASCADE        
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_order_payments_dataset` (
  `order_id` VARCHAR(50),
  `payment_sequential` int DEFAULT NULL,
  `payment_type` text,
  `payment_installments` int DEFAULT NULL,
  `payment_value` double DEFAULT NULL,
  FOREIGN KEY (order_id)
        REFERENCES olist_db.olist_orders_dataset(order_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_order_reviews_dataset` (
  `review_id` VARCHAR(50),
  `order_id` VARCHAR(50),
  `review_score` int DEFAULT NULL,
  `review_comment_title` text,
  `review_comment_message` text,
  `review_creation_date` datetime DEFAULT NULL,
  `review_answer_timestamp` datetime DEFAULT NULL,
  PRIMARY KEY (review_id),
  FOREIGN KEY (order_id)
        REFERENCES olist_db.olist_orders_dataset(order_id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- FACT AND DIMENSIONS ---
CREATE TABLE `olist_db`.`d_state` (
  `state_id` bigint NOT NULL AUTO_INCREMENT,
  `state` text,
  PRIMARY KEY (`state_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_state` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_city` (
  `city_id` bigint NOT NULL AUTO_INCREMENT,
  `state_id` bigint NOT NULL,
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
  `type_id` bigint NOT NULL,
  `payment_sequential` int NOT NULL,
  `payment_installments` int NOT NULL,
  `payment_value` double NOT NULL,
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
  `review_score` int NOT NULL,
  PRIMARY KEY (`review_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `olist_db`.`d_hour` (
  `hour_id` bigint NOT NULL AUTO_INCREMENT,
  `hour` int NOT NULL,
  PRIMARY KEY (`hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_hour` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_day` (
  `day_id` bigint NOT NULL AUTO_INCREMENT,
  `day` int NOT NULL,
  PRIMARY KEY (`day_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_day` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_month` (
  `month_id` bigint NOT NULL AUTO_INCREMENT,
  `month` int NOT NULL,
  PRIMARY KEY (`month_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_month` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_year` (
  `year_id` bigint NOT NULL AUTO_INCREMENT,
  `year` int NOT NULL,
  PRIMARY KEY (`year_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
ALTER TABLE `olist_db`.`d_year` AUTO_INCREMENT=1;

CREATE TABLE `olist_db`.`d_order` (
  `order_id` varchar(50) NOT NULL,
  `order_status` text,
  PRIMARY KEY (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;