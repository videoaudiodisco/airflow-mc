import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2", 
    schedule="10 0 * * 6#2", # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    # START_DATE : 2주전 월요일, END_DATE : 2주전 토요일
    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        env={
            'START_DATE' : '{{ (data_interval_start.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds }}',
            'END_DATE' : '{{ ( data_interval_start.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14) )| ds }}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )

