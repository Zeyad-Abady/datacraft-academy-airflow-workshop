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
        # TODO: GET https://api.chucknorris.io/jokes/random
        # Return a dict with keys "id" and "text".
        raise NotImplementedError

    @task
    def insert_row(joke: dict):
        # TODO: use PostgresHook(postgres_conn_id="postgres_default") to
        # INSERT the joke into lab1_staging. Use ON CONFLICT (id) DO NOTHING
        # so re-running the task is idempotent.
        raise NotImplementedError

    insert_row(fetch_joke())


lab1_first_dag()
