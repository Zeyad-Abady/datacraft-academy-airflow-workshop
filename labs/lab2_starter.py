"""
Lab 2 — Strengthen It (STARTER, in-class independent practice — Session 1)
----------------------------------------------------------------------------
Six minutes. Take your working Lab 1 DAG (lab1_starter.py / solutions/lab1_first_dag.py)
and harden it with three things you just learned:

  1. Replace the hardcoded host/user/password from Lab 1's insert_row task
     with a real Airflow Connection (postgres_default) via PostgresHook.
     That psycopg2.connect(...) call with the plaintext password goes away
     entirely.
  2. retries and retry_delay on the DAG's default_args — retries=3,
     retry_delay=timedelta(seconds=30).
  3. An on_failure_callback that logs (or, if you want to go further,
     alerts — a print statement is a fine stand-in for Slack/email today).

Then, to prove it works: point fetch_joke at a URL that will fail (a typo'd
domain works well), trigger the DAG, and watch it in the Grid View.

YOUR SUCCESS SIGNAL:
  - The task goes amber (up_for_retry) after the first failed attempt
  - Retries appear in the Grid View, spaced ~30 seconds apart
  - The task turns red (failed) once all 3 retries are exhausted
  - Your on_failure_callback fires — check the task logs for its message

To use: copy this file into ../dags/ (after Lab 1 is working), fill in the
TODOs, save, and trigger it. The full answer is in
../solutions/lab2_strengthen_it.py if you get stuck.
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import psycopg2


def alert_on_failure(context):
    # TODO: log something useful from `context` — e.g. task_instance,
    # exception, or run_id. A print() is fine for today; a real callback
    # would post to Slack or page on-call.
    raise NotImplementedError


default_args = {
    # TODO: set retries=3 and retry_delay=timedelta(seconds=30)
    # TODO: set on_failure_callback=alert_on_failure
}


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab2", "strengthen-it"],
    default_args=default_args,
)
def lab2_strengthen_it():

    @task
    def fetch_joke() -> dict:
        # TODO (for the failure demo only): temporarily point this at a bad
        # URL, e.g. "https://api.chucknorris.io/jokes/this-does-not-exist"
        # or a typo'd domain, so you can watch the retries happen.
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "text": data["value"]}

    @task
    def insert_row(joke: dict):
        # TODO: this is Lab 1's hardcoded connection — replace it.
        # Import PostgresHook (airflow.providers.postgres.hooks.postgres),
        # use PostgresHook(postgres_conn_id="postgres_default"), and call
        # hook.run(sql, parameters=(...)) instead of psycopg2.connect().
        conn = psycopg2.connect(
            host="postgres", dbname="airflow",
            user="airflow", password="airflow", port=5432,
        )
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO lab1_staging (id, text, loaded_at) "
                "VALUES (%s, %s, NOW()) ON CONFLICT (id) DO NOTHING;",
                (joke["id"], joke["text"]),
            )
        conn.close()

    insert_row(fetch_joke())


lab2_strengthen_it()
