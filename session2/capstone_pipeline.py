"""
    Session 1: TaskFlow (@dag/@task), Connections, retries via default_args
    Session 2: XComs (small references only), Sensors (deferrable), Datasets
               (outlet + a second DAG scheduled by it), Branching
               (@task.branch + skip-propagation fix), PostgresHook,
               SQLExecuteQueryOperator, idempotent SQL, TaskGroups

Two DAGs, on purpose — the same cross-DAG shape as datasets_demo.py:
    capstone_pipeline            the pipeline itself
    capstone_reporting_consumer  scheduled BY capstone_pipeline's Dataset,
                                  not by cron or a sensor

Requires the airflow-triggerer container (already in docker-compose.yaml
for the Sensors in Practice demo) and template_searchpath, which this DAG
sets itself — see the @dag(...) decorator below.

Run of show
-----------
1. Copy this file into dags/. Confirm two DAGs appear, both unpaused:
     - capstone_pipeline
     - capstone_reporting_consumer
2. capstone_pipeline waits on a signal file before doing anything — same
   mechanism as sensor_modes_demo.py, deferred mode this time (zero worker
   cost while it waits). Trigger the DAG, then unblock it from the HOST
   terminal:

       touch dags/.capstone_go_signal                                   # macOS/Linux
       New-Item -ItemType File -Force -Path dags\.capstone_go_signal    # Windows PowerShell

3. Watch the Graph View: bronze -> silver -> check_quality -> branches
   into quarantine_and_alert OR the gold group -> mark_success. Three
   TaskGroups collapse what would otherwise be seven-plus flat boxes into
   three clusters.
4. Note the branch target's task_id in the Graph View: gold.aggregate_reporting
   — group_id becomes a dotted prefix the moment a task lives inside a
   TaskGroup, exactly as taught.
5. Once gold.aggregate_reporting succeeds, check Browse -> Datasets for
   postgres://workshop-db/reporting_daily, then switch to
   capstone_reporting_consumer's Grid View — a new run appears on its own
   within one scheduler loop, no polling, no direct import between the DAGs.
6. Re-trigger capstone_pipeline (skip the signal-file wait by deleting and
   re-touching it) to prove every stage is safe to re-run: row counts stay
   stable instead of duplicating, which is the entire point of idempotent
   SQL.
7. Clean up the signal file before the next run:

       rm dags/.capstone_go_signal            # macOS/Linux
       Remove-Item dags\.capstone_go_signal   # Windows PowerShell

Note: FileSensor resolves `filepath` against the `fs_default` Connection —
ships as a default Airflow connection (type "File (Path)", empty base
path). Add it once via Admin -> Connections if it's missing.
"""

from datetime import datetime

import requests
import json

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

USGS_EARTHQUAKES_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"
)
SIGNAL_PATH = "/opt/airflow/dags/.capstone_go_signal"

# A Dataset is just a URI — a logical name, not a live connection. Marking
# it as an outlet on the gold stage is what lets a second DAG schedule
# itself off this pipeline with zero polling and zero direct coupling.
REPORTING_READY = Dataset("postgres://workshop-db/reporting_daily")


def fetch_from_api() -> list[dict]:
    # Plain helper, NOT @task — same reason as every other session2/ demo:
    # keep the fetched payload out of XCom entirely, and sidestep the
    # @task-calling-@task XComArg trap. bronze.load_raw calls this directly.
    r = requests.get(USGS_EARTHQUAKES_URL, timeout=10)
    r.raise_for_status()
    rows = []
    for feature in r.json()["features"]:
        props = feature["properties"]
        eventtime = datetime.fromtimestamp(props["time"] / 1000)
        rows.append(
            {
                "id": feature["id"],
                "event_date": eventtime.date().isoformat(),
                "payload": json.dumps(props),
            }
        )
    return rows


@dag(
    dag_id="capstone_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # triggered manually for the live demo
    catchup=False,
    tags=["capstone", "reference", "session2"],
    # Session 1: retries via default_args, applied to every task in the DAG.
    default_args={"retries": 2},
    # Fixes the sql="sql/upsert_clean.sql" bug from the Postgres slide:
    # sql/ is a top-level Docker volume, a sibling of dags/, not nested
    # under it — relative .sql paths only resolve once this is set.
    template_searchpath=["/opt/airflow/sql"],
)
def capstone_pipeline():
    # Session 2 — Sensors: deferrable mode, the zero-worker-cost variant.
    # Gate the whole pipeline on an external "go" signal instead of firing
    # immediately, same mechanism as sensor_modes_demo.py.
    wait_for_go_signal = FileSensor(
        task_id="wait_for_go_signal",
        filepath=SIGNAL_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 20,
        deferrable=True,
    )

    # Session 2 — TaskGroups: bronze/silver/gold, purely a Graph View
    # organization choice. None of these three groups change scheduling.
    with TaskGroup(group_id="bronze") as bronze:

        @task
        def load_raw() -> dict:
            # Session 2 — PostgresHook: Python-driven, used here because the
            # task needs to loop over parsed rows before each INSERT.
            hook = PostgresHook(postgres_conn_id="postgres_default")
            rows = fetch_from_api()
            # Session 2 — idempotent SQL: ON CONFLICT DO UPDATE, not
            # insert_rows(). The feed returns the same earthquake ids all
            # day, and raw_events has a (id, event_date) primary key — a
            # plain INSERT throws UniqueViolation on the second trigger.
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
            # Session 2 — XComs: return a small reference (a row count),
            # never the rows themselves. XCom is a Postgres row; a
            # DataFrame through it writes data through the metadata DB.
            return {"row_count": len(rows)}

        load_raw()

    # Session 2 — SQLExecuteQueryOperator: declared SQL, no Python logic.
    # sql/lab3/upsert_clean.sql already exists in the repo from Lab 3 and
    # is idempotent (ON CONFLICT ... DO UPDATE), so it's reused as-is here.
    with TaskGroup(group_id="silver") as silver:
        upsert_clean = SQLExecuteQueryOperator(
            task_id="upsert_clean",
            conn_id="postgres_default",
            sql="lab3/upsert_clean.sql",
            parameters={"run_date": "{{ ds }}"},
        )

    # Session 2 — Branching: @task.branch returns a task_id string, not
    # data. Note the target below is "gold.aggregate_reporting" — group_id
    # becomes a dotted prefix on every task_id inside a TaskGroup.
    @task.branch
    def check_quality(ds=None) -> str:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        null_count = hook.get_first(
            "SELECT COUNT(*) FROM clean_events WHERE event_date = %(d)s AND payload IS NULL;",
            parameters={"d": ds},
        )[0]
        return "quarantine_and_alert" if null_count > 0 else "gold.aggregate_reporting"

    @task
    def quarantine_and_alert(ds=None):
        # PostgresHook again — Python-driven, and idempotent by hand this
        # time: quarantine_events has no unique constraint to hang an
        # ON CONFLICT off, so NOT EXISTS does the same job.
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "INSERT INTO quarantine_events (id, event_date, payload, reason) "
            "SELECT id, event_date, payload, 'null_payload' "
            "FROM clean_events c "
            "WHERE c.event_date = %(d)s AND c.payload IS NULL "
            "AND NOT EXISTS ("
            "    SELECT 1 FROM quarantine_events q "
            "    WHERE q.id = c.id AND q.event_date = c.event_date"
            ");",
            parameters={"d": ds},
        )
        print("Pipeline quarantined this run — would alert on-call here.")

    # Session 2 — Datasets: outlets marks reporting_daily "updated" the
    # instant this task succeeds. capstone_reporting_consumer below
    # schedules off exactly this event.
    with TaskGroup(group_id="gold") as gold:
        aggregate_reporting = SQLExecuteQueryOperator(
            task_id="aggregate_reporting",
            conn_id="postgres_default",
            sql="lab3/reporting_aggregate.sql",
            parameters={"run_date": "{{ ds }}"},
            outlets=[REPORTING_READY],
        )

    @task(trigger_rule="none_failed_min_one_success")
    def mark_success():
        # Session 2 — Branching, the fix half: skipped status propagates
        # downstream, so a plain fan-in here would skip too. This trigger
        # rule is what makes the join run after either branch completes.
        print("Pipeline completed successfully for this run.")

    quality = check_quality()
    wait_for_go_signal >> bronze >> silver >> quality
    quality >> [quarantine_and_alert(), gold] >> mark_success()


capstone_pipeline()


@dag(
    dag_id="capstone_reporting_consumer",
    start_date=datetime(2024, 1, 1),
    schedule=[REPORTING_READY],  # scheduled BY the dataset, not by cron
    catchup=False,
    tags=["capstone", "reference", "session2"],
)
def capstone_reporting_consumer():
    @task
    def print_triggering_dataset_events(triggering_dataset_events=None):
        # Despite the parameter name, this dict is keyed by the dataset's
        # URI string, not a Dataset object — dataset here is already the URI.
        for dataset, events in triggering_dataset_events.items():
            for event in events:
                print(
                    f"reporting_daily refreshed by dataset '{dataset}', "
                    f"produced by DAG run '{event.source_dag_run.dag_id}' at "
                    f"{event.source_dag_run.data_interval_end}."
                )

    print_triggering_dataset_events()


capstone_reporting_consumer()
