from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    TbUseDaystatusView_sensor = SeoulApiDateSensor(
        task_id='TbUseDaystatusView_sensor',
        dataset_nm='TbUseDaystatusView',
        base_dt_col='DT',
        day_off = -1,
        poke_interval = 600,
        mode = 'reschedule',
    )

    cycleNewMemberRentInfoDay_sensor = SeoulApiDateSensor(
        task_id='cycleNewMemberRentInfoDay_sensor',
        dataset_nm='cycleNewMemberRentInfoDay',
        base_dt_col='DT',
        day_off = -1,
        poke_interval = 600,
        mode = 'reschedule',
    )

