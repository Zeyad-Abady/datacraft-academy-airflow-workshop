import requests
import json
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

USGS_EARTHQUAKES_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
)


def fetch_from_api() -> list[dict]:
    # Plain helper, NOT @task — same reason as xcom_dag.py: keep the fetched
    # payload out of XCom, and avoid the @task-calling-@task XComArg bug.
    # USGS earthquakes feed — a different API than Session 1's joke API.
    # Same shape: plain GET, no auth, JSON in, list[dict] out.
    r = requests.get(USGS_EARTHQUAKES_URL, timeout=10)
    r.raise_for_status()
    rows = []
    for feature in r.json()["features"]:
        props = feature["properties"]
        eventtime = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc)
        rows.append(
            {
                "id": feature["id"],
                "event_date": eventtime.date().isoformat(),
                "payload": json.dumps(props),
            }
        )

    return rows


@dag(
    dag_id="branching_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "branching"],
)
def branching_demo():
    @task
    def load_raw() -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        rows = fetch_from_api()  # returns a list of dicts
        # ON CONFLICT, not insert_rows(): the feed returns the same
        # earthquake ids on every re-run within the same day, and
        # raw_events has a (id, event_date) primary key. A plain
        # insert throws UniqueViolation the second time you trigger.
        for r in rows:
            hook.run(
                "INSERT INTO raw_events (id, event_date, payload) "
                "VALUES (%(id)s, %(event_date)s, %(payload)s) "
                "ON CONFLICT (id, event_date) DO UPDATE SET payload = EXCLUDED.payload;",
                parameters={
                    "id": r["id"],
                    "event_date": r["event_date"],
                    "payload": r["payload"],
                },
            )
        return {"table": "raw_events", "row_count": len(rows)}

    @task
    def check_quality(ref: dict) -> dict:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        null_count = hook.get_first(
            f"SELECT COUNT(*) FROM {ref['table']} WHERE payload IS NULL"
        )[0]
        return {"table": ref["table"], "null_count": null_count}

    @task.branch
    def choose_path(quality: dict) -> str:
        if quality["null_count"] > 0:
            return "route_to_quarantine"
        return "load_clean_table"

    @task
    def route_to_quarantine(): ...  # move rows, alert on-call

    @task
    def load_clean_table(): ...  # INSERT INTO clean_events SELECT ...

    quality = check_quality(load_raw())
    choose_path(quality) >> [route_to_quarantine(), load_clean_table()]


branching_demo()
