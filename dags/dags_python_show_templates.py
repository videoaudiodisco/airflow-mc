from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import random


with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025,3,10, tz='Asia/Seoul'),
    catchup=True
) as dag:

    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint

        pprint(kwargs)

    show_templates()