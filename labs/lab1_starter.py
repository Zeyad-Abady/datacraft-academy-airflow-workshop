"""
Lab 1 — Your First DAG (STARTER)
---------------------------------
Goal: fetch a joke from a public API and write it into the `lab1_staging`
table in Postgres.

For this lab, connect to Postgres the quick way: with psycopg2 and the
host/user/password typed directly into the file. It works, and it's the
fastest way to a running DAG today. It is *not* how you'd do this in a
real project — Lab 2 replaces this with a proper Airflow Connection, so
don't worry about fixing it yet.

To use: copy this file into ../dags/, fill in the TODOs, save, and watch it
appear in the Airflow UI within ~30 seconds.

Reference table (already created for you on first Postgres boot):
    lab1_staging (id TEXT PRIMARY KEY, text TEXT, loaded_at TIMESTAMP)
"""
from airflow.decorators import dag, task
from datetime import datetime
import requests
import psycopg2


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
        # TODO: connect with psycopg2.connect(host="postgres", dbname="airflow",
        # user="airflow", password="airflow", port=5432) and INSERT the joke
        # into lab1_staging. Use ON CONFLICT (id) DO NOTHING so re-running the
        # task is idempotent. Yes, this hardcodes a password — that's Lab 2's
        # job to fix, not this one's.
        raise NotImplementedError

    insert_row(fetch_joke())


lab1_first_dag()
