# Package Import
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

with DAG(
    dag_id='dags_python_with_postgres_hook_bulk_load',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule="0 7 * * *", # soul_api_opeartor.py와 동일하게 설정
    catchup=False
) as dag:

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)
        # bulk_load는 file_nm에 있는 파일을 읽어서 tbl_nm에 넣는 것.

    insrt_postgres = PythonOperator(
        task_id = 'insrt_postgres',
        python_callable= insrt_postgres,
        op_kwargs = {
            'postgres_conn_id': 'conn-db-postgres-custom',
            'tbl_nm' : 'TbUseDaystatusView_bulk1',
            'file_nm' : '/opt/airflow/files/TbUseDaystatusView/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/TbUseDaystatusView.csv'
        }
    )   
    # data_interval_end 는 batch가 도는 날짜, 시점 --> 즉, 오늘 날짜 폴더에서 가지고 온다.
    # 단 schedule 이 어떻게 설정되어있느냐에 따라 달라진다. 
    # 예를 들어, 0 7 * * *  으로 설정되어있고, 4.30 6:30에 돌린다면 data_interval_end는 4.29 07:00가 된다. data_interval_start는 4.28 07:00
    # 반면 4.30 7:30 에 돌린다면, data_interval_end는 4.30 07:00가 된다. data_interval_start는 4.29 07:00 