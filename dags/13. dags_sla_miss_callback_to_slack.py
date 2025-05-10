from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from config.sla_miss_callback_to_slack import sla_miss_callback_to_slack


with DAG(
    dag_id='dags_sla_miss_callback_to_slack',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    sla_miss_callback=sla_miss_callback_to_slack # 여기에서 미리 정의되어있는 함수를 불러온다.
) as dag:
    task_slp100_sla120 = BashOperator(
        task_id='task_slp100_sla120',
        bash_command='sleep 100',
        sla=timedelta(minutes=2)
    )

    task_slp100_sla180 = BashOperator(
        task_id='task_slp100_sla180',
        bash_command='sleep 100',
        sla=timedelta(minutes=3)
    ) # sla miss

    task_slp60_sla245 = BashOperator(
        task_id='task_slp60_sla245',
        bash_command='sleep 60',
        sla=timedelta(seconds=245)
    ) # sla miss 되어야 하지만, 실제 slack 알람이 오지 않음. sla miss 는 엄격하게 관리되지 않는다.
    # 또 한번 다시 돌려보면 정상적으로 되기도 한다.

    task_slp60_sla250 = BashOperator(
        task_id='task_slp60_sla250',
        bash_command='sleep 60',
        sla=timedelta(seconds=250)
    ) # sla miss

    task_slp100_sla120 >> task_slp100_sla180 >> task_slp60_sla245 >> task_slp60_sla250

    # 각 task마다 sla를 별도로 설정