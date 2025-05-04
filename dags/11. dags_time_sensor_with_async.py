import pendulum
from airflow import DAG
from airflow.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id="dags_time_sensor_with_async",
    start_date=pendulum.datetime(2025, 5, 1, 0, 0, 0),
    end_date=pendulum.datetime(2025, 5, 1, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,

    # dags_time_sensor.py와 동일한 DAG이지만, async로 변경되었다.
    # dag를 실행하면 running --> deffered 상태로 변경된다. 왜? task들이 대기하는 것이 아니라, triggerer를 기다리고 있기 때문에 
    # Browse --> Triggers 에 들어가면 trigger가 7개 생성되어있다.
    # Pools에 들어가도 개수 변화가 없다. 왜냐하면 deffered 상태이기 때문에 pool(slot)을 사용하지 않기 때문이다.

    # 작업완료되면 triggerer는 사라진다.
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )