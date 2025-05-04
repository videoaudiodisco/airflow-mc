import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id="dags_time_sensor",
    start_date=pendulum.datetime(2025, 5, 1, 0, 0, 0),
    end_date=pendulum.datetime(2025, 5, 1, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,

    # 5/1 0시부터 1시까지 10분마다 DAG이 실행 --> 0분 부터 60분까지 총 7번 실행
    # catchup이 true 이므로, dag을 실행하면 7개의 dagrun이 한꺼번에 실행된다.

    # 이 task들은 5분동안 실행되고, 5분 후에 완료
    # Admin --> Pools에 들어가면 pool이 7개 생성되어있다.
    
) as dag:
    sync_sensor = DateTimeSensor(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
        # 지금으로부터 5분이 지나길 기다리는 sensor --> 다른 task의 실행으로 이어진다.
    )

    # this sync_sensor task will pause the workflow at this point. 
    # When the task starts, it calculates a target time that is exactly 
    # 5 minutes into the future from that moment (in UTC). 
    # The sensor will then wait until that calculated time is reached 
    # before completing successfully and allowing any subsequent tasks 
    # in the DAG to run

