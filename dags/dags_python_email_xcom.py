from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:

    @task(task_id = 'soemthing_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['good', 'bad'])
    
    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'gyungyoonpark@gmail.com',
        subject = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리 결과' ,
        html_content = '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리 결과는 <br> \
            {{ ti.xcom_pull(task_ids="some_logic") }} 입니다. <br>',
        # task id로 xcom_pull 을 하면 해당 task_id의 return 값이 들어감
    )

    some_logic() >> send_email
