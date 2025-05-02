from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    
    # 각 날짜로 된 폴더에 TbUseDaystatusView.csv 파일이 존재하는지 확인
    # file(path) connection 등록할 때 path에 /opt/airflow/files 라고만 등록하면 된다. extra 없어짐
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id='TbUseDaystatusView',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='TbUseDaystatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbUseDaystatusView.csv',
        recursive=False,
        poke_interval=60, # 60초 마다 확인
        timeout=60*60*24, # 1일
        mode='reschedule'
    )
