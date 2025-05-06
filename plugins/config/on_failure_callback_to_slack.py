from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def on_failure_callback_to_slack(context):

    # context로 미리 정의되어있는 template 변수들을 가지고 온다.
    ti = context.get("ti") #  task instance 변수를 가지고 온다.
    dag_id = ti.dag_id
    task_id = ti.task_id
    err_msg = context.get("exception")
    batch_date = context.get("data_interval_end").in_timezone("Asia/Seoul")
    # 이 함수는 dag이 실패할때 호출되기때문에, 실패한 dag의 정보

    slack_hook = SlackWebhookHook(slack_webhook_conn_id = 'conn_slack_airflow_bot')
    text = "실패 알람"

    # slack building block 검색하면 block kit builder 를 사용하면 된다.
    blocks = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": f"*{dag_id}.{task_id} 실패 알람*"
			}
		},
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*배치 시간*: {batch_date}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*에러 내용*: {err_msg}"
                }
            ]
        }
    ]

    slack_hook.send(text=text, blocks=blocks)