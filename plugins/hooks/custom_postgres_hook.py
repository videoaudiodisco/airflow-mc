from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        # BaseHook을 상속할 때 get_conn을 쓰려면 반드시 재정의 필요. 안쓰면 안해도 됨
        # DB와의 연결 세션 객체인 self.postgres_conn 리턴한다.
        if not self.conn:
            airflow_conn = self.get_connection(self.postgres_conn_id)
            # 중요! 이것은 airflow ui 화면을 통해 등록한 postgres 로그인 정보가 담긴 변수
            # self.postgres_conn 과 다르다!
            self.host = airflow_conn.host
            self.user = airflow_conn.login
            self.password = airflow_conn.password
            self.dbname = airflow_conn.schema
            self.port = airflow_conn.port

            self.postgres_conn = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                dbname=self.dbname,
                port=self.port
            )
        return self.postgres_conn

    ## customized bulk_load method
    ## 원래 postgreshook의 bulk_load는 tsv 파일만 지원하지만, 이렇게 custom hook을 만들면 csv도 처리할 수 있다.
    def bulk_load(self, table_name, file_name, delimiter:str, is_header:bool, is_replace:bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상 파일 :' + file_name)
        self.log.info('적재 대상 테이블 :' + table_name)
        self.get_conn()  ## 여기에서 DB 연결을 한다.

        header = 0 if is_header else None
        if_exists = 'replace' if is_replace else 'append'
        file_df = pd.read_csv(file_name, delimiter=delimiter, header=header, encoding='utf-8')

        for col in file_df.columns:
            try:
                # string 문자열이 아닌 경우 continue
                # .str 메서드는 string 타입에만 적용 가능하고, 아니면 에러 발생 --> except로 넘어감
                file_df[col] = file_df[col].str.replace('\r\n', '') # 줄넘김 및 ^M 제거
                self.log.info(f"{table_name}.{col} : 개행문자 제거거")
                # csv를 바로 읽어오기 때문에, "난지도 1,2,3,4 주차장" 이 다른 컬럼으로 나뉘는 문제가 없어진다.
            except:
                continue

        self.log.info(f"적재 건수 : {len(file_df)}")
        uri = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        engine = create_engine(uri)

        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False
                       )