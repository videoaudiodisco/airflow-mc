# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    # airflow variable 등록할때 host를 http://openapi.seoul.go.kr:8088/	 로 등록해야 한다. (뒤에 :8088/ 붙여야 함)
    # port도 8088 로 입력
    tb_cycle_station_info = HttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/bikeList/1/10/',
        method='GET',
        headers={'Content-Type': 'application/json',
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()
