# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id = 'dags_timeout_example_1',
    start_date=pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    # 2 task를 합쳐도 40초 이기 때문에 dag run timeout이 발생하지 않는다.
    default_args = {
        'execution_timeout': timedelta(seconds=20), # task가 30초가 지나면 timeout이 발생한다.
        'email_on_failure': True,
        'email' : email_lst,
    }
) as dag:

    # 30초를 sleep하는 task인데, execution_timeout이 20초로 설정되어있다.
    # 따라서 failure가 발생한다.
    # 이 task가 fail이 되므로, 이메일 발송송
    bash_sleep_30 = BashOperator(
        task_id= 'bash_sleep_30',
        bash_command='sleep 30',
    )

    # 10초를 sleep하는 task인데, execution_timeout이 20초로 설정되어있다.
    # 따라서 정상적으로 수행된다.
    # 이 task는 성공이므로, 이메일 발송이 되지 않는다.
    bash_sleep_10 = BashOperator(
        trigger_rule='all_done',
        task_id= 'bash_sleep_10',
        bash_command='sleep 10',
    )

    bash_sleep_30 >> bash_sleep_10
    # bash_sleep_30이 실패하더라도, trigger_rull이 all_done이므로 bash_sleep_10은 수행된다.
    # 성공이든 실패이든 done이기만 하면 충족이 되므로