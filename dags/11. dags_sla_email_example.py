from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from airflow.models import Variable

# 지정된 시간을 지나서까지 수행완료가되면 sla miss 라고 표시
# failure가 되는 것은 아니다.


email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule='*/5 * * * *', # 매 10분마다 수행
    catchup=False,
    default_args={
        'sla': timedelta(seconds=70), # 70초가 지나면 sla miss로 표시됨
        'email': email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator(
        task_id='task_slp_30s_sla_70s',
        bash_command='sleep 30'
    )
    
    task_slp_60_sla_70s = BashOperator(
        task_id='task_slp_60_sla_70s',
        bash_command='sleep 60'
    )

    task_slp_10s_sla_70s = BashOperator(
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s',
        bash_command='sleep 10',
        sla=timedelta(seconds=30)
    )

    task_slp_30s_sla_70s >> task_slp_60_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s

    # 이렇게 되면, 첫번째 task 말고는 모두 sla miss가 발생한다.
