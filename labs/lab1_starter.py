"""
Lab 1 — Your First DAG (STARTER)
---------------------------------
Goal: fetch a joke from a public API and write it into the `lab1_staging`
table in Postgres.

To use: copy this file into ../dags/, fill in the TODOs, save, and watch it
appear in the Airflow UI within ~30 seconds.

Reference table (already created for you on first Postgres boot):
    lab1_staging (id TEXT PRIMARY KEY, text TEXT, loaded_at TIMESTAMP)
"""
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab1"],
)
def lab1_first_dag():

    @task
    def fetch_joke() -> dict:
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "text": data["value"]}

    @task
    def insert_row(joke: dict):
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "INSERT INTO lab1_staging (id, text, loaded_at) "
            "VALUES (%s, %s, NOW()) ON CONFLICT (id) DO NOTHING;",
            parameters=(joke["id"], joke["text"]),
        )

    insert_row(fetch_joke())


lab1_first_dag()
