from airflow.sensors.base import BaseSensorOperator
from airflow.hooks.base import BaseHook

'''
서울시 공공데이터 API 추출시 특정 날짜 컬럼을 조사하여 
배치 날짜 기준 전날 데이터가 존재하는지 체크하는 센서 
1. 데이터셋에 날짜 컬럼이 존재하고 
2. API 사용시 그 날짜 컬럼으로 ORDER BY DESC 되어 가져온다는 가정하에 사용 가능
3. 10. dags_python_sensor.py 에서 사용한 check_api_update() 함수와 거의 동일
다만, 이렇게 sensor로 만들어 놓으면, 다른 dag에서도 재사용 가능하다는 장점이 있다.

'''


class SeoulApiDateSensor(BaseSensorOperator):
    template_fields = ('endpoint', )

    def __init__(self, dataset_nm, base_dt_col, day_off=0, **kwargs):
        '''
        dataset_nm: 서울시 공공데이터 포털에서 센싱하고자 하는 데이터셋 명
        base_dt_col: 센싱 기준 컬럼 (yyyy.mm.dd... or yyyy/mm/dd... 형태만 가능)
        day_off: 배치일 기준 생성여부를 확인하고자 하는 날짜 차이를 입력 (기본값: 0)
        '''
    
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt_col = base_dt_col
        self.day_off = day_off

    def poke(self, context):
        import requests
        import json
        from dateutil.relativedelta import relativedelta

        connection = BaseHook.get_connection(self.http_conn_id)
        url = f'{connection.host}{self.endpoint}/1/100/' ## ui에서 host를 :port/까지 같이 정의했으므로 이렇게 변경
        self.log.info(f'request url:{url}')
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')


        last_dt = row_data[0].get(self.base_dt_col) # 첫 행에서 날짜 부분의 값을 가져온다. 2025/05/01 이런 형태임
        last_date = last_dt.replace('.', '-').replace('/', '-')
        try:
            import pendulum
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f"{self.base_dt_col} 컬럼의 날짜 형식이 YYYY.MM.DD 또는 YYYY/MM/DD 형태가. {last_dt}")

        # 보통 서울시 공공데이터는 어제일자로 업데이트가 되므로, 그것이 제대로 되었는지 확인. 오늘 날짜로 하면 안된다.
        # 그런 경우에 day_off를 1로 설정하면 된다.
        search_ymd = (context.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta(days=self.day_off)).strftime('%Y-%m-%d')
        
        
        if last_date >= search_ymd:
            print(f'생성 확인(기준 날짜: {search_ymd} / API Last 날짜: {last_date})')
            return True
        else:
            print(f'공공데이터 Update 미완료(기준준 날짜: {search_ymd} / API Last 날짜: {last_date})')
            return False