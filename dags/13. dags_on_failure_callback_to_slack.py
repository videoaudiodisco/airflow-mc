from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from config.on_failure_callback_to_slack import on_failure_callback_to_slack 


with DAG(
    dag_id='dags_on_failure_callback_to_slack',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='0 * * * *',
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_slack, # 이전에 만든 함수를 여기에서 사용
        'execution_timeout': timedelta(seconds=60) # 60초가 지나도록 task가 완료되지 않으면 실패로 간주
    }

) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90',
    ) # 이건 실패

    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_ext_1',
        bash_command='exit 1'
    ) # exit 0가 아니면 모두 실패, all done 때문에 먼저 task가 실패해도 실행됨

    task_slp_90 >> task_ext_1