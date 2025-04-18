from airflow import DAG
import pendulum
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # task id가 같으면 원래 오류가 나지만, group이 다르면 괜찮다.

    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)
    
    @task_group(group_id = 'first_group')
    def group_1():
        # docstring --> tooltip으로 나옴
        ''' task_group decorator를 사용한 첫번째 그룹'''

        @task(task_id='inner_function1')
        def inner_func1():
            print('first task of the first task group')
        
        inner_func2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': 'second task of the first task group'}
        )

        inner_func1() >> inner_func2
        # task_group decorator를 사용한 첫번째 그룹

    with TaskGroup(group_id='second_group', tooltip = 'this is the second group') as group_2:
        # tool tip 이 docstring에 나옴
        ''' task group deco를 사용하지 않으면, 여기에 적은 docstring은 표시 되지 않는다.'''

        @task(task_id='inner_function1')
        def inner_func1():
            print('first task of the second task group')
        
        inner_func2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': 'second task of the second task group'}
        )

        inner_func1() >> inner_func2
        # task_group context manager를 사용한 두번째 그룹

    group_1() >> group_2
    # task group도 순서를 정해줄 수 있다.
