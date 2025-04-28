# Package Import
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:


    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=["172.28.0.3", "5432", "gypark", "gypark", "gypark"]
    )

    # 중요 : postgres_custom을 5430:5432로 만들었는데, 여기서 5432를 써야 한다.
    # 이유 : 파이썬 코드가 실행되는 곳이 laptop이나 local 이라면 5430으로 해야 함
    # 그러나, 만약 같은 network 상의 다른 컨테이너로부터 라면 5432로 해야한다.
    # airflow는 webserver와 scheduler가 같은 network 상에 있기 때문에 5432로 해야 한다.
    # 이건 connection hook 만들때도 동일!
        
    insrt_postgres