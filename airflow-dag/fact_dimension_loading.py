import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session
from airflow.models import XCom

import os
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from datetime import timedelta

from helpers import storage

db_name = os.getenv('MYSQL_NAME')


@provide_session
def on_success(context, session=None):
    dag_id = context['dag_run'].dag_id
    # Cleanup xcom
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def clear_db(ds, **kwargs):
    clear_db_func(db_name+'.f_sales')
    clear_db_func(db_name+'.d_order')
    clear_db_func(db_name+'.d_review')
    clear_db_func(db_name+'.d_product')
    clear_db_func(db_name+'.d_product_category')
    clear_db_func(db_name+'.d_payment')
    clear_db_func(db_name+'.d_payment_type')
    clear_db_func(db_name+'.d_city')
    clear_db_func(db_name+'.d_state')
    clear_db_func(db_name+'.d_hour')
    clear_db_func(db_name+'.d_day')
    clear_db_func(db_name+'.d_month')
    clear_db_func(db_name+'.d_year')

def set_autoincrement(ds, **kwargs):
    query = '''
    ALTER TABLE `olist_db`.`d_state` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_city` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_payment_type` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_payment` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_product_category` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_hour` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_day` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_month` AUTO_INCREMENT=1;
    ALTER TABLE `olist_db`.`d_year` AUTO_INCREMENT=1;'''
    query_execute(query,'f_sales')


def state_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_state (state) 
    SELECT customers.state AS state FROM (SELECT DISTINCT(customer_state) AS state FROM olist_db.olist_customers_dataset) AS customers;'''
    query_execute(query,'d_state')

def city_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_city (city, state_id)
    (SELECT customers.customer_city AS city, state_id FROM (SELECT DISTINCT customer_city, customer_state FROM olist_db.olist_customers_dataset AS olist_customers_dataset
    INNER JOIN olist_db.olist_orders_dataset AS olist_orders_datase ON olist_orders_datase.customer_id = olist_customers_dataset.customer_id) AS customers
    INNER JOIN olist_db.d_state AS state ON state.state = customers.customer_state);'''
    query_execute(query,'d_city')      

def payment_type_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_payment_type (payment_type)
    (SELECT payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments);'''
    query_execute(query,'d_payment_type')  

def payment_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_payment (type_id, payment_sequential, payment_installments, payment_value)
    (SELECT type_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
    INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type);'''
    query_execute(query,'d_payment') 

def product_category_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_product_category (category_name)
    (SELECT DISTINCT product_category_name  AS category_name FROM olist_db.olist_products_dataset AS products_dataset
    INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);'''
    query_execute(query,'d_product_category') 

def product_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_product (product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty)
    (SELECT DISTINCT products_dataset.product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty FROM olist_db.olist_products_dataset AS products_dataset
    LEFT JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name
    INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);'''
    query_execute(query,'d_product')      

def review_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_review (review_id, review_score)
    (SELECT review_id, review_score FROM olist_db.olist_order_reviews_dataset);'''
    query_execute(query,'d_review')
        
def order_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_order (order_id, order_status)
    (SELECT order_id, order_status FROM olist_db.olist_orders_dataset);'''
    query_execute(query,'d_order')
        
def hour_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_hour (`hour`)
    SELECT HOUR(order_approved_at) AS `hour` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query,'d_hour')
        
def day_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_day (`day`)
    SELECT DAY(order_approved_at) AS `day` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query,'d_day')
        
def month_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_month (`month`)
    SELECT MONTH(order_approved_at) AS `month` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query,'d_month')
        
def year_dim(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_year (`year`)
    SELECT YEAR(order_approved_at) AS `year` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query,'d_year')
    
def sales_fact(ds, **kwargs):
    # CREATE TEMPORARY TABLES
    query = '''
    DROP TABLE IF EXISTS olist_db.temp_city;
    CREATE TABLE olist_db.temp_city
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
    '''
    print('temp_city')
    query_execute(query,'temp_city')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_payment;
    SET @rownr=0;
    CREATE TABLE olist_db.temp_payment
    SELECT @rownr:=@rownr+1 AS payment_id, type_id, order_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
    INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type;
    '''
    print('temp_payment')
    query_execute(query,'temp_payment')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_review;
    CREATE TABLE olist_db.temp_review
    SELECT review_id, order_id, review_score FROM olist_db.olist_order_reviews_dataset;
    '''
    print('temp_review')
    query_execute(query,'temp_review')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_hour;
    SET @rownr=0;
    CREATE TABLE olist_db.temp_hour
    SELECT @rownr:=@rownr+1 AS hour_id, order_id, HOUR(order_approved_at) AS `hour` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;
    '''
    print('temp_hour')
    query_execute(query,'temp_hour')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_day;
    SET @rownr=0;
    CREATE TABLE olist_db.temp_day
    SELECT @rownr:=@rownr+1 AS day_id, order_id, DAY(order_approved_at) AS `day` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;
    '''
    print('temp_day')
    query_execute(query,'temp_day')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_month;
    SET @rownr=0;
    CREATE TABLE olist_db.temp_month
    SELECT @rownr:=@rownr+1 AS month_id, order_id, MONTH(order_approved_at) AS `month` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;
    '''
    print('temp_month')
    query_execute(query,'temp_month')

    query = '''
    DROP TABLE IF EXISTS olist_db.temp_year;
    SET @rownr=0;
    CREATE TABLE olist_db.temp_year
    SELECT @rownr:=@rownr+1 AS year_id, order_id, YEAR(order_approved_at) AS `year` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;
    '''
    print('temp_year')
    query_execute(query,'temp_year')                        

    # LOAD FACT SALES
    query = '''
    SET GLOBAL interactive_timeout=120;
    SET GLOBAL connect_timeout=120;

    INSERT INTO olist_db.f_sales (order_id, product_id, city_id, payment_id, review_id, hour_id, day_id, month_id, year_id, price)
    (SELECT 
    orders_dataset.order_id
    , product_id
    , city_id
    , payment_id
    , review_id
    , hour_id
    , day_id
    , month_id
    , year_id
    , order_items_dataset.price
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
    WHERE order_approved_at IS NOT NULL);
    '''
    query_execute(query,'f_sales')
    
    # DROP TEMP TABLES
    query = '''
    DROP TABLE IF EXISTS olist_db.temp_city;
    DROP TABLE IF EXISTS olist_db.temp_payment;
    DROP TABLE IF EXISTS olist_db.temp_review;
    DROP TABLE IF EXISTS olist_db.temp_hour;
    DROP TABLE IF EXISTS olist_db.temp_day;
    DROP TABLE IF EXISTS olist_db.temp_month;
    DROP TABLE IF EXISTS olist_db.temp_year;
    '''
    query_execute(query,'f_sales')    

# Execute query        
def query_execute(query, table_name):
    try:
        connection = storage.engine_connect()
        connection.execute(query)
        result = connection.execute('SELECT COUNT(*) FROM olist_db.{db_table_name};'.format(db_table_name = table_name))
        print('Row count: ' + str([{value for value in row} for row in result if result is not None][0]))
    except BaseException as e:
        raise ValueError(e) 

# Delete DB
def clear_db_func(db_table_name):
    try:
        print('delete DB: {db_table_name}'.format(db_table_name = db_table_name))
        storage.delete_db(db_table_name)
        return True
    except BaseException as e:
        raise ValueError(e)       

# Set Airflow args
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    dag_id='load_dim_fact',
    default_args=args,
    max_active_runs=1,
    schedule_interval='00 17 * * 1',
    catchup=False,
    on_success_callback = on_success,) as dag:

    clear_db = PythonOperator(
        task_id='clear_db',
        provide_context=True,
        python_callable=clear_db,
    )

    set_autoincrement = PythonOperator(
        task_id='set_autoincrement',
        provide_context=True,
        python_callable=set_autoincrement,
    )    

    state_dim = PythonOperator(
        task_id='state_dim',
        provide_context=True,
        python_callable=state_dim,
    )    

    city_dim = PythonOperator(
        task_id='city_dim',
        provide_context=True,
        python_callable=city_dim,
    )
    
    payment_type_dim = PythonOperator(
        task_id='payment_type_dim',
        provide_context=True,
        python_callable=payment_type_dim,
    )
    
    payment_dim = PythonOperator(
        task_id='payment_dim',
        provide_context=True,
        python_callable=payment_dim,
    )
    
    product_category_dim = PythonOperator(
        task_id='product_category_dim',
        provide_context=True,
        python_callable=product_category_dim,
    )
    
    product_dim = PythonOperator(
        task_id='product_dim',
        provide_context=True,
        python_callable=product_dim,
    )
    
    review_dim = PythonOperator(
        task_id='review_dim',
        provide_context=True,
        python_callable=review_dim,
    )
    
    order_dim = PythonOperator(
        task_id='order_dim',
        provide_context=True,
        python_callable=order_dim,
    )
    
    hour_dim = PythonOperator(
        task_id='hour_dim',
        provide_context=True,
        python_callable=hour_dim,
    )
    
    day_dim = PythonOperator(
        task_id='day_dim',
        provide_context=True,
        python_callable=day_dim,
    )
    
    month_dim = PythonOperator(
        task_id='month_dim',
        provide_context=True,
        python_callable=month_dim,
    )
    
    year_dim = PythonOperator(
        task_id='year_dim',
        provide_context=True,
        python_callable=year_dim,
    )
    
    sales_fact = PythonOperator(
        task_id='sales_fact',
        provide_context=True,
        python_callable=sales_fact,
    )     

clear_db >> set_autoincrement >> state_dim >> city_dim >> payment_type_dim >> payment_dim >> product_category_dim >> product_dim >> review_dim >> order_dim >> hour_dim >> day_dim >> month_dim >> year_dim >> sales_fact