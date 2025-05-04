from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum


dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
    dag_id='dags_dataset_producer_2',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "producer_2 completed"',
        outlets=[dags_dataset_producer_2],
    )