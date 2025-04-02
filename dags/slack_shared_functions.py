from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from jinja2 import Template
from logging_config import logger
from airflow.models import Variable

SLACK_BOT_TOKEN = Variable.get("slack_secret_token")
SLACK_CHANNEL = Variable.get("alert_channel")

FAILURE_SLACK_MESSAGE = Template(
    """
    *ATTENTION!* <!channel> The DAG *{{ ti.dag_id }}* has *just* failed executing! :exclamation:
    
    >*Task:* `{{ ti.task_id }}`
    >*Execution date:* `{{ ds }}`
    >*State:* `{{ ti.state }}`
    
    More information about the error in <http://localhost:9090/dags/{{ ti.dag_id }}/grid|*here*>.
    """
)

def slack_notifier(context):
    """
    Sends a notification to a specified Slack channel when a DAG fails.
    
    Parameters:
    context (dict): Context dictionary provided by Airflow containing task instance (ti) 
                    and other useful information such as execution date (ds).
    """
    try:
        message = FAILURE_SLACK_MESSAGE.render(ti=context.get('ti'), ds=context.get('ds'))
        response = WebClient(token=SLACK_BOT_TOKEN).chat_postMessage(
            channel=SLACK_CHANNEL, text=message, mrkdwn=True
        )
        logger.info(f"Slack notification sent successfully: {response['ts']}")
    except SlackApiError as e:
        logger.error(f"Failed to send Slack notification: {e.response.get('error', 'Unknown error')}")
