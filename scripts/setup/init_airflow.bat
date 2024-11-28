@echo off
REM Initialize Airflow environment
set AIRFLOW_HOME=%~dp0airflow
set PYTHONPATH=%~dp0

REM Install requirements
pip install -r requirements-airflow.txt

REM Initialize the database
airflow db init

REM Create admin user
airflow users create ^
    --username admin ^
    --firstname Admin ^
    --lastname User ^
    --role Admin ^
    --email admin@example.com ^
    --password admin

REM Start Airflow webserver
start cmd /k "airflow webserver -p 8080"

REM Start Airflow scheduler
start cmd /k "airflow scheduler"
