from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "email_on_failure": False,  # wire a Slack callback instead
}


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
)
def resilient_dag():
    @task(retries=5)  # override for this task only
    def flaky_api_call(): ...

    flaky_api_call()


resilient_dag()
