from airflow import DAG
from airflow.sensors.python import PythonSensor
import pendulum
from airflow.hooks.base import BaseHook

# 강의에 나온 코로나 대신, 서울시 공공데이터의 한강공원 주차장 일별 이용 현황을 사용
# TbUseDaystatusView 이고, 그에 맞게 코드도 수정

# 매일 dag 이 돌면서 실행될때마다, 기준일 기준으로 업데이트가 되었는지 확인
# 해당 데이터는 매일 1회 업데이트가 되며, 어제까지 업데이트가 된다.
# 즉, data_interval_end - 1일 기준으로 업데이트가 되었는지 확인해야 한다.
# 또는 data_interval_start 기준으로 데이터의 날짜가 있는지 확인해도 된다.

with DAG(
    dag_id='dags_python_sensor',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule='10 1 * * *',
    catchup=False
) as dag:
    
    # 이 함수는 거의 그대로 모든 데이터에 사용 가능하다.
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwargs):
        # http_conn_id = 'openapi.seoul.go.kr'
        # endpoint = api_key/json/TbUseDaystatusView
        # base_dt_col = 'DT' ## 날짜 컬럼명
        import requests
        import json
        from dateutil import relativedelta

        connection = BaseHook.get_connection(http_conn_id)
        # url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100/' ## 원코드
        url = f'{connection.host}{endpoint}/1/100/' ## ui에서 host를 :port/까지 같이 정의했으므로 이렇게 변경
        # 데이터가 날짜 내림차순으로 되어있으므로 첫 100개 행만 가져와도 된다.
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        print(f'row_data: {row_data}')

        last_dt = row_data[0].get(base_dt_col) # 첫 행에서 날짜 부분의 값을 가져온다. 2025/05/01 이런 형태임
        last_date = last_dt.replace('.', '-').replace('/', '-')
        try:
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f"{bast_dt_col} 컬럼의 날짜 형식이 YYYY.MM.DD 또는 YYYY/MM/DD 형태가. {last_dt}")

        # 한강주차장 데이터는 어제일자로 업데이트가 되므로, 그것이 제대로 되었는지 확인. 오늘 날짜로 하면 안된다.
        yesterday_ymd = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').subtract(days=1).format('YYYY-MM-DD')
        if last_date >= yesterday_ymd:
            print(f'생성 확인(배치 시작 날짜: {yesterday_ymd} / API Last 날짜: {last_date})')
            return True
        else:
            print(f'공공데이터 Update 미완료(배치 시작 날짜: {yesterday_ymd} / API Last 날짜: {last_date})')
            return False
        

    sensor_task = PythonSensor(
        task_id = 'sensor_task',
        python_callable=check_api_update,
        op_kwargs = {
            'http_conn_id': 'openapi.seoul.go.kr',
            'endpoint': '{{var.value.apikey_openapi_seoul_go_kr}}/json/TbUseDaystatusView',
            'base_dt_col': 'DT'
        },
        poke_interval = 60 * 10, # 10분마다 확인
        mode = 'reschedule',
    )