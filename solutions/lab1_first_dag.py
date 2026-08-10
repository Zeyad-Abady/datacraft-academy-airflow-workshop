"""
Lab 1 — Your First DAG (SOLUTION). Matches Session 1, slide 13.

Connects to Postgres the quick way — psycopg2, with host/user/password
typed directly into the file. Deliberately not best practice: Lab 2
(slide 16) replaces this with a real Airflow Connection via PostgresHook.
"""
from airflow.decorators import dag, task
from datetime import datetime
import requests
import psycopg2


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["lab1", "solution"])
def lab1_first_dag():

    @task
    def fetch_joke() -> dict:
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "text": data["value"]}

    @task
    def insert_row(joke: dict):
        # Quick and dirty for Lab 1 — host/user/password typed right here.
        # We replace this with a real Airflow Connection in Lab 2.
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


lab1_first_dag()
