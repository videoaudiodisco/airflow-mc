from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_2',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1), # DAG run timeout 설정 1분
    default_args={
        'execution_timeout': timedelta(seconds=40), # task timeout 설정 40초
        'email_on_failure': True, # 이것은 task가 실패했을 때만 이메일 발송이 된다. (dag가 실패한 것으로는 X)
        'email': email_lst
    }
) as dag:
    
    # 35초를 sleep하는 task인데, execution_timeout이 40초로 설정되어있다.
    # 따라서 task는 정상적으로 수행된다.
    bash_sleep_35 = BashOperator(
        task_id='bash_sleep_35',
        bash_command='sleep 35',
    )

    # 36초를 sleep하는 task인데, execution_timeout이 40초로 설정되어있다.
    # 따라서 task는 정상적으로 수행되어야 하지만!
    # dag run timeout이 1분이어서, 이 task를 수행하는 도중에 dag run timeout이 발생한다.
    # --> 그럼 이 task는 skipped 상태로 변경된다.
    bash_sleep_36 = BashOperator(
        trigger_rule='all_done',
        task_id='bash_sleep_36',
        bash_command='sleep 36',
    )

    # 직전 task에서 dag run timeout이 발생했기 때문에, Task is in the 'None' state.
    bash_go = BashOperator(
        task_id='bash_go',
        bash_command='exit 0',
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go
    # dag 자체는 fail로 된다. 근데 task는 fail이 아니므로, 이메일 발송이 되지 않는다.
    