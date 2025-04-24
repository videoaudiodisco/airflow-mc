# Package Import
from airflow import DAG
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow.decorators import task
import pendulum


with DAG(
    dag_id = 'dags_seoul_api_operator',
    schedule= "0 7 * * *",
    start_date= pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    
    '''한강공원 주차장 일별 이용 현황'''
    hanriver_parking_daily_status = SeoulApiToCsvOperator(
        task_id='TbUseDaystatusView',
        dataset_nm='TbUseDaystatusView',
        path='/opt/airflow/files/TbUseDaystatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbUseDaystatusView.csv'
    )
    
    '''서울특별시 공공자전거 신규가입자 정보(일별)'''
    seoul_bike_daily_status = SeoulApiToCsvOperator(
        task_id='cycleNewMemberRentInfoDay',
        dataset_nm='cycleNewMemberRentInfoDay',
        path='/opt/airflow/files/cycleNewMemberRentInfoDay/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='cycleNewMemberRentInfoDay.csv'
    )
    # data_interval_end 는 batch가 도는 날짜, 시점.

    hanriver_parking_daily_status >> seoul_bike_daily_status

"""
docker 에 들어가서 path 설정을 별도로 해줘야 한다.
지금 이 dag 들은 work container 에서 작동하는데, 당연히 docker가 내려가면 데이터가 사라진다.
따라서 work container에 있는 데이터를 host machine에 저장하기 위해서
docker-compose.yml 에서 volumes 설정을 해줘야 한다.

airflow folder에서 plugins, dags 폴더는 이미 연결이 되어있다.
yaml 파일에 volumes에 보면 : 기준으로 왼쪽이 wsl, 오른쪽이 container의 경로이다.
volumes에 아래를 추가한다.
{AIRFLOW_PROJ_DIR:-.}/airflow-mc/files:/opt/airflow/files


docker에 airflow-airflow-worker-1 컨테이너 안에 들어가려면
docker exec -it [container name] /bin/bash

"""