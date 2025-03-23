import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_bash_with_template", # 화면에서 보이는 값값
    schedule="0 0 * * *", # 분 시 일 월 요일
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),


) as dag:
    bash_t1 = BashOperator(
        task_id='bash_t1',
        bash_command='echo "data_interval_end: {{data_interval_end}}"',
    )

    bash_t2 = BashOperator(
        task_id='bash_t2',
        env={
            'START_DATE' : '{{data_interval_start | ds}}', # YYYY-MM-DD 형식으로 출력
            'END_DATE' : '{{data_interval_end | ds}}',
            
        },
        bash_command='echo &START_DATE && echo $END_DATE',
    )

    bash_t1 >> bash_t2