import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_operator", # 화면에서 보이는 값값
    schedule="0 0 * * *", # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example1', 'example2']
) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo whoami',
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        bash_command='echo $HOSTNAME',
    )

    bash_t1 >> bash_t2