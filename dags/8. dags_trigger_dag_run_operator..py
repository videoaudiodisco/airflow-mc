# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule='30 3 * * *',
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    # trigger_dag_task = TriggerDagRunOperator(
    #     task_id='trigger_dag_task',
    #     # trigger_dag_id='dags_python_operator',
    #     trigger_dag_id = 'dags_email_operator',
    #     trigger_run_id=None,
    #     execution_date='{{data_interval_start}}',
    #     reset_dag_run=True,
    #     wait_for_completion=False,
    #     poke_interval=60,
    #     allowed_states=['success'],
    #     failed_states=None
    #     )

    trigger_dag_task = TriggerDagRunOperator(
        task_id = 'trigger_dag_task',
        trigger_dag_id= 'dags_python_operator', # 이 task가 trigger 하는 dag
        trigger_run_id= None, # None이면 자동으로 run_id가 생성됨 --> manual로 나온다. 
        execution_date= '{{data_interval_start}}',
        # run id 가 manual__{{execution_date}}로 생성됨
        reset_dag_run=True, # 이미 run id 값이 있는 경우에도 재수행을 하는지.
        wait_for_completion=False, # True이면 trigger_dag_id의 dag_run이 완료될 때까지 대기함
        poke_interval=60, 
        allowed_states=['success'], # trigger_dag_id의 dag_run이 success 상태일 때만 trigger됨
        failed_states=None,
    )


    start_task >> trigger_dag_task




    # start_task >> trigger_dag_task
