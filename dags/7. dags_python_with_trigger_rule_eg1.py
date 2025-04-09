from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 4, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo "bash upstream 1"'
    )

    @task(task_id = 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException("python upstream 1 error")
        # alrflowexception을 발생시키면 이 task는 실패로 간주됨 
        # 실패하더라도 done으로 간주됨

    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        print("python upstream 2")

    @task(task_id = 'python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print("python downstream 1")

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()

    # python_downstream_1 이 all_done 이므로, 위에서 하나가 실패가 나도 돌아갈 것
