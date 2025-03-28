import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator



with DAG(
    dag_id="dags_bash_with_macro_eg1", 
    schedule="10 0 L * *", # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    # START_DATE : 전월 말일, END_DATE : 1일 전
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={
            'START_DATE' : '{{ data_interval_start.in_time_zone("Asia/Seoul") | ds}}',
            'END_DATE' : '{{ data_interval_end.in_time_zone("Asia/Seoul") - macros.timedelta(days=1) | ds}}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )


    bash_task_1 