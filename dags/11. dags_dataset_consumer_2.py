from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")
dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id='dags_dataset_consumer_2',
    schedule= [dags_dataset_producer_1, dags_dataset_producer_2], ## schedule에 이렇게 리스트 형태로 들어간다.
    start_date=pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ti.run_id}} && echo "execute when producer_1 and producer_2 is completed"',
    )