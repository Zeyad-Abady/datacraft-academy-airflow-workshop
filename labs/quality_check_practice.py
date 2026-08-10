"""
Post-Session Practice — Add a Quality Check (STARTER, optional, between sessions)
------------------------------------------------------------------------------------
Not a numbered lab — this is optional practice to do after Session 1, before
Session 2. About an hour, and it pays off directly: Session 2 builds on this
exact branching pattern for the Lab 3 capstone.

Goal: extend Lab 2 (lab2_starter.py) with a data-quality check that branches
to a quarantine task when data looks bad.

To use: copy this file into ../dags/ (after you have a working Lab 2),
fill in the TODOs, save, and trigger it twice — once with clean data and
once with data that should fail the check.
"""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["practice", "quality-check"],
)
def quality_check_practice():

    @task
    def fetch_joke() -> dict:
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "text": data["value"]}

    @task
    def insert_row(joke: dict) -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "INSERT INTO lab1_staging (id, text, loaded_at) "
            "VALUES (%s, %s, NOW()) ON CONFLICT (id) DO NOTHING;",
            parameters=(joke["id"], joke["text"]),
        )
        # TODO: return a small dict describing what you just wrote —
        # e.g. {"id": joke["id"], "text_length": len(joke["text"])}
        raise NotImplementedError

    @task.branch
    def check_quality(ref: dict) -> str:
        # TODO: decide pass/fail. Example rule: fail if text_length == 0.
        # Return the task_id of the branch to take: "quarantine" or "promote".
        raise NotImplementedError

    @task
    def quarantine():
        print("Row quarantined — would alert on-call here.")

    @task
    def promote():
        print("Row promoted — quality check passed.")

    ref = insert_row(fetch_joke())
    check_quality(ref) >> [quarantine(), promote()]


quality_check_practice()
