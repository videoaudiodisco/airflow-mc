import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command="echo start &&"
                "echo xcom_pushed"
                "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} &&"
                "echo complete"
    )
    ## bash echo를 한 경우 맨 마지막 값이 함수의 return 값으로 xcom_push에 저장됨
    ## 즉, 다음 함수에서 task_ids를 통해 xcom_pull을 할 경우, echo complete이 return_value로 저장됨

    bash_pull = BashOperator(
        task_id='bash_pull',
        env = {
            'pushed_value' : "{{ ti.xcom_pull(key='bash_pushed') }}",  ## key는 반드시 string 형식으로 입력
            'return_value' : "{{ ti.xcom_pull(task_ids='bash_push') }}",
        },
        bash_command="echo $pushed_value && echo $return_value",
        do_xcom_push=False,
    )

    bash_push >> bash_pull

