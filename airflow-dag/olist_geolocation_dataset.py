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
table_name = 'olist_geolocation_dataset'
file_full_path = '/usr/local/airflow/files/'+table_name+'.csv'

@provide_session
def on_success(context, session=None):
    dag_id = context['dag_run'].dag_id
    # Cleanup xcom
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def clear_db(ds, **kwargs):
    clear_db_func(db_name+'.'+table_name)

def load_data(ds, **kwargs):
    load_data_func(file_full_path, db_name+'.'+table_name)

# Truncate DB
def clear_db_func(db_table_name):
    try:
        print('Truncate DB: {db_table_name}'.format(db_table_name = db_table_name))
        storage.truncate_db(db_table_name)
        return True
    except BaseException as e:
        raise ValueError(e)       

# Save data into DB
def load_data_func(file_path, db_table_name):

    print('File name: {file_path}'.format(file_path = file_path))
    print('DB table name: {db_table_name}'.format(db_table_name = db_table_name))

    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name,storage.engine_connect(),index=False,if_exists="append",schema=db_name)

        connection=storage.engine_connect()
        result = connection.execute('SELECT COUNT(*) FROM {db_table_name};'.format(db_table_name= db_table_name))
        print('Row count: ' + str([{value for value in row} for row in result if result is not None][0]))
        return True
    except SQLAlchemyError as e:
        raise ValueError(str(e.__dict__['orig']))

# Set Airflow args
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    dag_id=table_name,
    default_args=args,
    max_active_runs=1,
    schedule_interval='00 12 * * 1',
    catchup=False,
    on_success_callback = on_success,) as dag:

    clear_db = PythonOperator(
        task_id='clear_db',
        provide_context=True,
        python_callable=clear_db,
    )

    load_data = PythonOperator(
        task_id='load_data',
        provide_context=True,
        python_callable=load_data,
    )

clear_db >> load_data