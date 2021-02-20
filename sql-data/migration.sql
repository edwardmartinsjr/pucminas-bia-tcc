DROP DATABASE IF EXISTS `olist_db`;
CREATE DATABASE `olist_db`;

CREATE TABLE `olist_db`.`product_category_name_translation` (
  `product_category_name` text,
  `product_category_name_english` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_geolocation_dataset` (
  `geolocation_zip_code_prefix` text,
  `geolocation_lat` double DEFAULT NULL,
  `geolocation_lng` double DEFAULT NULL,
  `geolocation_city` text,
  `geolocation_state` text
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
  `seller_zip_code_prefix` text,
  `seller_city` text,
  `seller_state` text,
  PRIMARY KEY (seller_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `olist_db`.`olist_customers_dataset` (
  `customer_id` VARCHAR(50),
  `customer_unique_id` VARCHAR(50),
  `customer_zip_code_prefix` text,
  `customer_city` text,
  `customer_state` text,
  PRIMARY KEY (customer_id)
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
