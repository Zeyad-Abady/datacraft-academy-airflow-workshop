"""
Lab 3 — Capstone: Multi-Stage Postgres Pipeline (STARTER)
-------------------------------------------------------------
Kicked off in Session 2, finished at home.

Build a DAG that:
  1. Pulls data from a public API (or generate fake data) — Raw stage
  2. Loads into `raw_events`, partitioned by event_date
  3. Transforms into `clean_events` with idempotent SQL (see ../sql/lab3/)
  4. Aggregates into `reporting_daily`
  5. Runs a quality check that branches to quarantine on failure
  6. Sends an alert (print/log stand-in is fine) on failure

Reference tables (already created for you on first Postgres boot):
    raw_events         (id, event_date, payload JSONB, loaded_at)
    clean_events        (id, event_date, payload JSONB, cleaned_at)
    quarantine_events    (id, event_date, payload JSONB, reason, quarantined_at)
    reporting_daily      (report_date PK, event_count, generated_at)

SQL files to fill in: ../sql/lab3/upsert_clean.sql, ../sql/lab3/reporting_aggregate.sql
"""
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab3", "capstone"],
    default_args={"retries": 2},
)
def lab3_capstone_pipeline():

    @task
    def extract_raw(**context) -> dict:
        # TODO: pull data from a public API of your choice, or generate
        # fake rows. Return a small reference dict (e.g. row_count).
        raise NotImplementedError

    @task
    def load_raw(ref: dict, **context) -> dict:
        # TODO: use PostgresHook to INSERT into raw_events for
        # event_date = {{ ds }}. Use ON CONFLICT to stay idempotent.
        raise NotImplementedError

    # TODO: uncomment once sql/lab3/upsert_clean.sql is filled in
    # upsert_clean = SQLExecuteQueryOperator(
    #     task_id="upsert_clean",
    #     conn_id="postgres_default",
    #     sql="sql/lab3/upsert_clean.sql",
    #     parameters={"run_date": "{{ ds }}"},
    # )

    # TODO: uncomment once sql/lab3/reporting_aggregate.sql is filled in
    # aggregate_reporting = SQLExecuteQueryOperator(
    #     task_id="aggregate_reporting",
    #     conn_id="postgres_default",
    #     sql="sql/lab3/reporting_aggregate.sql",
    #     parameters={"run_date": "{{ ds }}"},
    # )

    @task.branch
    def check_quality(**context) -> str:
        # TODO: query clean_events for the run date, decide pass/fail.
        raise NotImplementedError

    @task
    def quarantine_and_alert():
        print("Pipeline quarantined this run — would alert on-call here.")

    @task
    def mark_success():
        print("Pipeline completed successfully for this run.")

    ref = load_raw(extract_raw())
    # ref >> upsert_clean >> aggregate_reporting >> check_quality() >> [quarantine_and_alert(), mark_success()]


lab3_capstone_pipeline()
