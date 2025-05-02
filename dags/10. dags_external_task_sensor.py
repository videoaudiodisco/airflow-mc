from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State 

with DAG(
    dag_id='dags_external_task_sensor',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False

) as dag:
    external_task_sensor_a = ExternalTaskSensor(
        task_id='external_task_sensor_a',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_a', # task_a의 상태를 확인하기 위한 sensor
        allowed_states=[State.SKIPPED], # task_a가 skip 상태일 때, 이 task가 sucess가 된다.
        execution_delta=timedelta(hours=6), # dags_branch_python_operator 의 schedule과의 차이
        poke_interval=10        #10초
    )

    external_task_sensor_b = ExternalTaskSensor(
        task_id='external_task_sensor_b',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_b',
        failed_states=[State.SKIPPED], # task_b가 skip 상태일 때, 이 task가 failed가 된다.
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10초
    )

    external_task_sensor_c = ExternalTaskSensor(
        task_id='external_task_sensor_c',
        external_dag_id='dags_branch_python_operator',
        external_task_id='task_c',
        allowed_states=[State.SUCCESS], # task_c가 success 상태일 때, 이 task가 sucess가 된다.
        execution_delta=timedelta(hours=6),
        poke_interval=10        #10초
    )