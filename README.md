# Puc Minas BIA TCC
Puc Minas BIA TCC - ETL running on top of Apache Airflow

## Master Status:
[![CircleCI](https://circleci.com/gh/edwardmartinsjr/pucminas-bia-tcc/tree/main.svg?style=shield&circle-token=f186ba11e963ce943269eb2e03f22c2ffce56262)](https://circleci.com/gh/edwardmartinsjr/pucminas-bia-tcc/tree/main)

## Architecture:
![](architecture.png?raw=true)

## Olist DB Diagram:
![](olist_db.png?raw=true)

## Configuration:
Install application dependencies:
- `pip install -r requirements.txt`

MySQL:
- `docker pull mysql/mysql-server:8.0`
- `docker run -d --name=mysql1 -e MYSQL_ROOT_PASSWORD=mysql -p 3306:3306 mysql/mysql-server:8.0 --default-authentication-plugin=mysql_native_password`
- `docker exec -it mysql1 mysql -uroot -p`
- `ALTER USER 'root'@'localhost' IDENTIFIED BY 'mysql';`
- `CREATE USER 'sa'@'localhost' IDENTIFIED BY 'sa';`
- `CREATE USER 'sa'@'%' IDENTIFIED BY 'sa';`
- `GRANT ALL ON *.* TO 'sa'@'localhost';`
- `GRANT ALL ON *.* TO 'sa'@'%';`
- `flush privileges;`
- `SELECT host, user FROM mysql.user;`
- `set global local_infile=true;`

## Run:
Docking Apache Airflow:
- `docker build -t airflow-1_10_12_basic . `
- `docker run --name pucminas_bia_tcc_machine -d --mount src="C:\Users\Edward\Projects\dataset",target=/usr/local/airflow/files,type=bind --env MYSQL_USER=sa --env MYSQL_PASSWORD=sa --env MYSQL_HOST=192.168.178.17 --env MYSQL_PORT=3306 --env MYSQL_NAME=olist_db -p 80:8080 airflow-1_10_12_basic`
- `docker exec -it pucminas_bia_tcc_machine /bin/bash`
- `vi airflow.cfg`
- `load_examples = False`
- `exit`
- `docker stop pucminas_bia_tcc_machine`
- `docker start pucminas_bia_tcc_machine`
- `docker exec -it pucminas_bia_tcc_machine /bin/bash`
- `airflow resetdb`

Running at Apache Airflow:
- Add DAG `airflow-dag/` to DAGs folder `/usr/local/airflow/dags/` 
- `docker cp airflow-dag/. pucminas_bia_tcc_machine:/usr/local/airflow/dags`

![](docker.png?raw=true)

## Running tests:
- `python ./airflow-dag/storage_test.py -v`

## Python version:
Python 3.8.7

