from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist2
import random

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025,3,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
        
    regist_t2 = PythonOperator(
        task_id= 'regist_t2',
        python_callable=regist2,
        op_args=['gypark', 'man', 'kr', 'seoul'],
        op_kwargs= {'country': 'kr',
                  'city': 'seoul' }
                
    )
    
    regist_t2