from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import psycopg2

CATEGORIES = ["dev", "food", "movie", "sport", "travel"]


@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def dynamic_mapping_demo():
    @task
    def fetch_joke(
        category: str,
    ) -> dict: ...  # same shape as Lab 1's fetch_joke, plus a category param

    @task
    def insert_row(joke: dict):  # same PostgresHook pattern as Lab 2
        ...

    insert_row.expand(joke=fetch_joke.expand(category=CATEGORIES))


dynamic_mapping_demo()
