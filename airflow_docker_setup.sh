#!/usr/bin/env bash

#####################################################
#                                                   #
#           AIRFLOW SETUP SCRIPT                    #
#           @searchs Ola Ajibode                    #
#####################################################


# Check Airflow memory requirements
docker run --rm "debian:buster-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'

# Download docker-compose.yaml
wget https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml


### Wht's included in this install
# 1. airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers 
#   the task instances once their dependencies are complete.

# 2. airflow-webserver - The webserver is available at http://localhost:8080.

# 3. airflow-worker - The worker that executes the tasks given by the scheduler.

# 4. airflow-init - The initialization service.

# 5. flower - The flower app for monitoring the environment. It is available at http://localhost:5555.

# 6. postgres - The database.

# 7. redis - The redis - broker that forwards messages from scheduler to worker.

# Create local directories 
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env


# Initialize the database
docker-compose up airflow-init


# Cleanup Airflow and envrionment
docker-compose down --volumes --remove-orphans

# Airflow Wrapper script
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.4/airflow.sh'
chmod +x airflow.sh

# Enter interactive Bash shell
./airflow.sh bash

# Enter interactive Python shell
./airflow.sh python


# UI : http://localhost:8080. The default account has the login airflow and the password airflow.

# Retrieve a pool list
ENDPOINT_URL="http://localhost:8080/"
curl -X GET  \
    --user "airflow:airflow" \
    "${ENDPOINT_URL}/api/v1/pools"

# Stop and Delete all containers
docker-compose down --volumes --rmi all
