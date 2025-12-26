#!/bin/bash
set -e
if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r requirements.txt    

fi
if [ -e "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
fi
$(command -v airflow) db upgrade
exec airflow webserver