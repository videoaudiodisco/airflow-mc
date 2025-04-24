from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd 


class SeoulApiToCsvOperator(BaseOperator):


    def __init__(self, dataset_nm, path, file_name, base_dt = None, **kwargs):
        super().__init__(**kwargs)
        self.dataset_nm = dataset_nm
        self.path = path
        self.file_name = file_name
        self.base_dt = base_dt
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm

        def execute(self, context):
            import os

            # 이전에 ui에서 등록한 variable을 가져옴
            connection = BaseHook.get_connection(self.http_conn_id)

            ## 이전에 ui 에서 등록할때 port 까지 같이 host에 등록했기 때문에 수정 필요?
            self.base_url = f"http://{connection.host}:{connection.port}/{self.endpoit}"

            total_row_df = pd.DataFrame()
            start_row = 1
            end_row = 1000

            # 1000개 씩 가져오되, 마지막 개수가 1000개 미만이면 더이상 데이터 없으므로 중단
            while True:
                self.log.info(f'시작 : {start_row}')
                self.log.info(f'끝 : {end_row}')
                row_df = self._call_api(self.base_url, start_row, end_row)
                total_row_df = pd.concat([total_row_df, row_df])
                if len(row_df) < 1000:
                    break
                else:
                    start_row = end_row + 1
                    end_row += 1000

            if not os.path.exists(self.path):
                os.system(f'mkdir -p {self.path}')
            total_row_df.to_csv(f'{self.path}/{self.file_name}', index=False, encoding='utf-8')

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {
            'Content-Type': 'application/json',
            'charset' : 'utf-8',
            'Accept' : '*/*'
        }

        request_url = f"{base_url}/{start_row}/{end_row}"
        if self.base_dt is not None:
            request_url = f"{base_url}/{start_row}/{end_row}/{self.base_dt}"

        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text) # dictionary 형태로 변환


        ## 이 부분은 어떤 데이터 가지고 오느냐에 따라 따름
        # TbUseDaystatusView : 한강 주차장 일별 이용 현황  사용해보기
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df= pd.DataFrame(row_data)

        return row_df