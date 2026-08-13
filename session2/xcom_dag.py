from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
import requests
import json


USGS_EARTHQUAKES_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
)


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def xcom_demo():
    @task
    def fetch_from_api() -> list[dict]:
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

    check_quality(load_raw())


xcom_demo()
