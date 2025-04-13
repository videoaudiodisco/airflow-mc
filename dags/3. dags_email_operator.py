from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator



with DAG(
    dag_id="dags_email_operator",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to = 'gyungyoonpark@gmail.com',
        subject='airflow test email',
        html_content='airflow task completed'
            )
