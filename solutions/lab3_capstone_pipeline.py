"""
Lab 3 — Capstone: Multi-Stage Postgres Pipeline (SOLUTION)
Matches Session 2, slides 8-10. Uses ../sql/lab3/*.sql for the SQL stages.
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
    tags=["lab3", "capstone", "solution"],
    default_args={"retries": 2},
    # sql/ is its own top-level Docker volume (a sibling of dags/, not
    # nested under it — see docker-compose.yaml), so a relative sql=
    # path below only resolves once this is set.
    template_searchpath=["/opt/airflow/sql"],
)
def lab3_capstone_pipeline():

    @task
    def extract_raw() -> dict:
        r = requests.get("https://api.chucknorris.io/jokes/random", timeout=10)
        r.raise_for_status()
        data = r.json()
        return {"id": data["id"], "payload": data["value"]}

    @task
    def load_raw(record: dict, **context) -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        run_date = context["ds"]
        hook.run(
            "INSERT INTO raw_events (id, event_date, payload, loaded_at) "
            "VALUES (%(id)s, %(event_date)s::date, %(payload)s, NOW()) "
            "ON CONFLICT (id, event_date) DO UPDATE SET payload = EXCLUDED.payload;",
            parameters={
                "id": record["id"],
                "event_date": run_date,
                "payload": record["payload"],
            },
        )
        return {"row_count": 1}

    upsert_clean = SQLExecuteQueryOperator(
        task_id="upsert_clean",
        conn_id="postgres_default",
        sql="lab3/upsert_clean.sql",
        parameters={"run_date": "{{ ds }}"},
    )

    aggregate_reporting = SQLExecuteQueryOperator(
        task_id="aggregate_reporting",
        conn_id="postgres_default",
        sql="lab3/reporting_aggregate.sql",
        parameters={"run_date": "{{ ds }}"},
    )

    @task.branch
    def check_quality(**context) -> str:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        run_date = context["ds"]
        null_count = hook.get_first(
            "SELECT COUNT(*) FROM clean_events WHERE event_date = %(d)s AND payload IS NULL;",
            parameters={"d": run_date},
        )[0]
        return "quarantine_and_alert" if null_count > 0 else "mark_success"

    @task
    def quarantine_and_alert():
        print("Pipeline quarantined this run — would alert on-call here.")

    @task
    def mark_success():
        print("Pipeline completed successfully for this run.")

    ref = load_raw(extract_raw())
    ref >> upsert_clean >> aggregate_reporting >> check_quality() >> [quarantine_and_alert(), mark_success()]


lab3_capstone_pipeline()
