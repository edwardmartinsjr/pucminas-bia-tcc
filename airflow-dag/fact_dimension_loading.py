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
    # Dimensions will be deleted by cascade
    clear_db_func(db_name+'.f_sales') 

def state(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_state (state) 
    SELECT customers.state AS state FROM (SELECT DISTINCT(customer_state) AS state FROM olist_db.olist_customers_dataset) AS customers;'''
    query_execute(query)

def city(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_city (city, state_id)
    (SELECT customers.customer_city AS city, state_id FROM (SELECT DISTINCT olist_customers_dataset.customer_id, customer_city, customer_state FROM olist_db.olist_customers_dataset AS olist_customers_dataset
    INNER JOIN olist_db.olist_orders_dataset AS olist_orders_datase ON olist_orders_datase.customer_id = olist_customers_dataset.customer_id) AS customers
    INNER JOIN olist_db.d_state AS state ON state.state = customers.customer_state);'''
    query_execute(query)       

def payment_type(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_payment_type (payment_type)
    (SELECT payments.payment_type as payment_type FROM (SELECT DISTINCT(payment_type) AS payment_type FROM olist_db.olist_order_payments_dataset) AS payments);'''
    query_execute(query)   

def payment(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_payment (type_id, payment_sequential, payment_installments, payment_value)
    (SELECT type_id, payment_sequential, payment_installments, payment_value FROM olist_db.olist_order_payments_dataset AS payments_dataset
    INNER JOIN olist_db.d_payment_type AS payment_type ON payment_type.payment_type = payments_dataset.payment_type);'''
    query_execute(query)  

def product_category(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_product_category (category_name)
    (SELECT DISTINCT product_category_name  AS category_name FROM olist_db.olist_products_dataset AS products_dataset
    INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);'''
    query_execute(query)  

def product(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_product (product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty)
    (SELECT DISTINCT products_dataset.product_id, category_id, product_name_lenght, product_description_lenght, product_photos_qty FROM olist_db.olist_products_dataset AS products_dataset
    LEFT JOIN olist_db.d_product_category AS product_category ON product_category.category_name = products_dataset.product_category_name
    INNER JOIN olist_db.olist_order_items_dataset AS order_items_dataset ON order_items_dataset.product_id = products_dataset.product_id);'''
    query_execute(query)       

def review(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_review (review_id, review_score)
    (SELECT review_id, review_score FROM olist_db.olist_order_reviews_dataset);'''
    query_execute(query)
        
def order(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_order (order_id, order_status)
    (SELECT order_id, order_status FROM olist_db.olist_orders_dataset);'''
    query_execute(query)
        
def hour(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_hour (`hour`)
    SELECT HOUR(order_approved_at) AS `hour` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query)
        
def day(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_day (`day`)
    SELECT DAY(order_approved_at) AS `day` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query)
        
def month(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_month (`month`)
    SELECT MONTH(order_approved_at) AS `month` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query)
        
def year(ds, **kwargs):
    query = '''INSERT INTO olist_db.d_year (`year`)
    SELECT YEAR(order_approved_at) AS `year` FROM olist_db.olist_orders_dataset
    WHERE order_approved_at IS NOT NULL;'''
    query_execute(query)          

# Execute query        
def query_execute(query):
    try:
        connection = storage.engine_connect()
        result = connection.execute(query)
        print('Row count: ' + str([{value for value in row} for row in result if result is not None][0]))
    except SQLAlchemyError as e:
        raise ValueError(str(e.__dict__['orig']))    

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
    dag_id='load_dim',
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

    state = PythonOperator(
        task_id='state',
        provide_context=True,
        python_callable=state,
    )    

    city = PythonOperator(
        task_id='city',
        provide_context=True,
        python_callable=city,
    )    
    payment_type = PythonOperator(
        task_id='payment_type',
        provide_context=True,
        python_callable=payment_type,
    )    
    payment = PythonOperator(
        task_id='payment',
        provide_context=True,
        python_callable=payment,
    )    
    product_category = PythonOperator(
        task_id='product_category',
        provide_context=True,
        python_callable=product_category,
    )    
    product = PythonOperator(
        task_id='product',
        provide_context=True,
        python_callable=product,
    )    
    review = PythonOperator(
        task_id='review',
        provide_context=True,
        python_callable=review,
    )    
    order = PythonOperator(
        task_id='order',
        provide_context=True,
        python_callable=order,
    )    
    hour = PythonOperator(
        task_id='hour',
        provide_context=True,
        python_callable=hour,
    )    
    day = PythonOperator(
        task_id='day',
        provide_context=True,
        python_callable=day,
    )    
    month = PythonOperator(
        task_id='month',
        provide_context=True,
        python_callable=month,
    )    
    year = PythonOperator(
        task_id='year',
        provide_context=True,
        python_callable=year,
    )    

clear_db >> state >> city >> payment_type >> payment >> product_category >> product >> review >> order >> hour >> day >> month >> year 