from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
    dag_id='dags_dataset_producer_1',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "producer_1 completed"',
        outlets=[dags_dataset_producer_1],
    )