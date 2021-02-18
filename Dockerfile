# Base Image
FROM python:3.8.5-slim
LABEL maintainer="Docker Community"

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=1.10.12
ARG AIRFLOW_HOME=/usr/local/airflow
ARG MYSQL_USER='root'
ARG MYSQL_PASSWORD='mysql'
ARG MYSQL_HOST='127.0.0.1'
ARG MYSQL_PORT='3306'
ARG MYSQL_NAME='olist_db'

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Export DB connection environment variable
ENV MYSQL_USER=${MYSQL_USER}
ENV MYSQL_PASSWORD=${MYSQL_PASSWORD}
ENV MYSQL_HOST=${MYSQL_HOST}
ENV MYSQL_PORT=${MYSQL_PORT}
ENV MYSQL_NAME=${MYSQL_NAME}

# Install dependencies and tools
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    autoconf \
    automake \
    pkg-config \
    libtool \ 
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get clean

COPY ./requirements-for-apache-airflow.txt /requirements-for-apache-airflow.txt

# Upgrade pip
# Create airflow user 
# Install apache airflow with subpackages
RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install pyxlsb \
    pip install xlrd \
    pip install xlsxwriter \
    pip install mysqlclient \
    pip install -Iv apache-airflow==${AIRFLOW_VERSION} --constraint /requirements-for-apache-airflow.txt

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./entrypoint.sh ./entrypoint.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./entrypoint.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Set the username to use
USER airflow

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}

# Create the dags folder which will contain the DAGs
RUN mkdir dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute the entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]