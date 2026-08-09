"""Lab 2 — Add a Quality Check (SOLUTION). Matches Session 2, slides 3-7."""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["lab2", "solution"])
def lab2_quality_check():

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
        return {"id": joke["id"], "text_length": len(joke["text"])}

    @task.branch
    def check_quality(ref: dict) -> str:
        if ref["text_length"] == 0:
            return "quarantine"
        return "promote"

    @task
    def quarantine():
        print("Row quarantined — would alert on-call here.")

    @task
    def promote():
        print("Row promoted — quality check passed.")

    ref = insert_row(fetch_joke())
    check_quality(ref) >> [quarantine(), promote()]


lab2_quality_check()
