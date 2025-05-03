from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator


with DAG(
    dag_id="dags_branch_python_operator",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    def select_random():
        import random
        item_list = ['A', 'B', 'C']
        selected_item = random.choice(item_list)

        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']
        
    ## BranchPythonOperator에 들어가는 python_callable은 return 으로 task_id를 return
    ## 그 아래에는 각각의 task_id를 지니는 PythonOperator가 존재해야 함
    # BranchPythonOperator : 분개하는 역할을 하는 operator
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random,
    )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'},
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'},
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'},
    )   

    python_branch_task >> [task_a, task_b, task_c]
    # 만약 select_random에서 A가 선택되면 task_a는 success, task_b와 task_c는 skip
    # select_random에서 B,C 가 선택되면 task _a는 skip, task_b와 task_c는 success