from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import os
import pandas as pd

# MySQL
db_user = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD') 
db_host = os.getenv('MYSQL_HOST') 
db_port = os.getenv('MYSQL_PORT') 
db_name = os.getenv('MYSQL_NAME')

# Get connection
def get_conn():
    return 'mysql://%s:%s@%s:%s/%s?charset=utf8mb4&local_infile=1' % (db_user, db_password, db_host, db_port, db_name)
    
# Set MySQL engine connection
def engine_connect():
    return create_engine(get_conn(), pool_size=10, max_overflow=200)

# Create database session (SQLAlchemy Object Relational Mapper - ORM )
Session = sessionmaker()
Session.configure(bind=engine_connect())

# Load database session
def create_object_session(objects):
    session = Session()
    
    for obj in objects:
        session.add(obj)

    return session

# Get session
def get_session():
    return Session()

def truncate_db(table):
    # Truncate all table data
    # Engine-level in transactional context
    with engine_connect().begin() as conn:
        conn.execute('TRUNCATE TABLE ' + table + ';')

def delete_db(table):
    # Delete all table data
    # Engine-level in transactional context
    with engine_connect().begin() as conn:
        conn.execute('DELETE FROM ' + table + ';')

# Open excel file
def open_file(path_to_open):
    try:
        return open(path_to_open, 'rb')
    except BaseException as e:
        print(e)
        
def data_dump(df, file_name):
    try:
        df.to_csv('/usr/local/airflow/files/'+file_name+'.csv', header=True, index=False)
    except BaseException as e:
        print(e)

def data_load(file_name):
    try:
        df = pd.read_csv('/usr/local/airflow/files/'+file_name+'.csv')
        return df
    except BaseException as e:
        print(e)
        
# Save data into DB
def load_data_into_db(df, db_name, table_name):
    try:
        df.to_sql(table_name,engine_connect(),index=False,if_exists="append",schema=db_name)

        connection=engine_connect()
        result = connection.execute('SELECT COUNT(*) FROM {db_table_name};'.format(db_table_name= db_name+'.'+table_name))
        print('Row count: ' + str([{value for value in row} for row in result if result is not None][0]))
        return True
    except SQLAlchemyError as e:
        raise ValueError(str(e.__dict__['orig']))

# Extract data from CSV
def extract_data_from_csv(file_path):

    print('File name: {file_path}'.format(file_path = file_path))
    try:
        df = pd.read_csv(file_path)

        return df
    except BaseException as e:
        raise ValueError(e)           
