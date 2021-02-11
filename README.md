# Puc Minas BIA TCC
Puc Minas BIA TCC - ETL running on top of Apache Airflow

## Master Status:
[![CircleCI](https://circleci.com/gh/edwardmartinsjr/pucminas-bia-tcc/tree/main.svg?style=shield&circle-token=f186ba11e963ce943269eb2e03f22c2ffce56262)](https://circleci.com/gh/edwardmartinsjr/pucminas-bia-tcc/tree/main)

## Architecture:
![](architecture.png?raw=true)

## Configuration:
Install application dependencies:
- `pip install -r requirements.txt`

## Run:
Docking Apache Airflow:
- `docker build -t airflow-1_10_12_basic . `
- `docker run --name pucminas_bia_tcc_machine -d --mount src="C:\Users\Edward\Projects\dataset",target=/usr/local/airflow/files,type=bind --env MYSQL_USER=root --env MYSQL_PASSWORD=mysql --env MYSQL_HOST=127.0.0.1 --env MYSQL_PORT=3306 --env MYSQL_NAME=olist_db -p 80:8080 airflow-1_10_12_basic`

Running at Apache Airflow:
- Add DAG `airflow-dag/` to DAGs folder `/usr/local/airflow/dags/` 

## Running tests:
- `python ./airflow-dag/storage_test.py -v`

## Python version:
Python 3.8.7

