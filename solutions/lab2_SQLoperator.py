"""
Lab 2 — Strengthen It (SOLUTION). Matches Session 1, slide 16.

Hardens the Lab 1 DAG: replaces Lab 1's hardcoded psycopg2 connection with
a real Airflow Connection (postgres_default via PostgresHook), adds
retries + retry_delay, and adds an on_failure_callback. Point fetch_joke's
URL at a bad domain to trigger and watch the retry/failure behavior live.
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests


def alert_on_failure(context):
    ti = context["task_instance"]
    print(
        f"[ALERT] Task {ti.task_id} in DAG {ti.dag_id} failed on run "
        f"{context['run_id']}. In production this would post to Slack "
        f"or page on-call instead of just printing."
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": alert_on_failure,
}


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab2", "strengthen-it", "solution"],
    default_args=default_args,
)
def lab2_SQLoperator():

    @task
    def fetch_joke() -> dict:
        # Swap this URL for a bad domain (e.g. add a typo) to trigger the
        # retry/failure demo live: the task should go amber, retry three
        # times ~30s apart, then turn red, firing alert_on_failure.
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "text": data["value"]}

    insert_row = (
        SQLExecuteQueryOperator(
            task_id="insert_row",
            conn_id="postgres_default",
            sql="""
        INSERT INTO lab1_staging (id, text, loaded_at)
        VALUES (%(id)s, %(text)s, NOW())
        ON CONFLICT (id) DO NOTHING;
        """,
            parameters={
                "id": "{{ ti.xcom_pull(task_ids='fetch_joke')['id'] }}",
                "text": "{{ ti.xcom_pull(task_ids='fetch_joke')['text'] }}",
            },
        ),
    )

    fetch_joke() >> insert_row


lab2_SQLoperator()
