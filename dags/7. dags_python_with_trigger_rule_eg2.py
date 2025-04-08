from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    @task.branch(task_id ='branching')
    def random_branch():
        import random
        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)
        if selected_item == 'A':
            return 'task_A'
        elif selected_item == 'B':
            return 'task_B'
        else:
            return 'task_C'
        
    task_a = BashOperator(
        task_id='task_A',
        bash_command='echo upstream1'
    )

    @task(task_id='task_B')
    def task_b():
        print('task_B 정상처리')

    @task(task_id='task_C')
    def task_c():
        print('task_C 정상처리')

    @task(task_id='task_D', trigger_rule='none_skipped')
    def task_d():
        print('task_D 정상처리')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()