"""
Instructor demo — Datasets: Scheduling on Data, Not Time
==========================================================
Session 2, "Datasets in Practice" slide (right after the Datasets concept
slide). Not a student lab — copy this into dags/ and run it live.

Two DAGs, wired together only through a shared Dataset. No ExternalTaskSensor,
no cron guessing, no direct import between the two DAG files. Trigger the
producer and watch the consumer fire on its own.

Uses the raw_events table from init-db/01_workshop_tables.sql — the same
table the "In Code — Passing a Reference, Not the Data" slide writes to — so
there is nothing new to set up before this demo.

Run of show
-----------
1. Copy this file into dags/. Confirm two DAGs appear, both unpaused:
     - raw_events_producer_demo
     - raw_events_consumer_demo
2. Open Browse -> Datasets in the UI. Find the postgres://workshop-db/
   raw_events entry. It should show zero consuming dataset events so far.
3. Trigger raw_events_producer_demo manually.
4. Switch to the Grid View for raw_events_consumer_demo. Within one scheduler
   loop (a few seconds — no manual trigger) a new run appears on its own.
5. Open the consumer task's log. print_triggering_dataset_events prints
   which producer DAG run triggered it, pulled straight from the
   triggering_dataset_events context variable.
6. Trigger the producer a second time to show this is not a one-off: every
   successful producer run creates a new dataset event, and the consumer
   fires every time.

The punchline: the producer and consumer never call each other, never share
a cron schedule, and never poll one another. They share exactly one thing —
a Dataset URI — and Airflow does the rest. This is how most multi-team
production Airflow deployments wire pipelines together today, instead of
ExternalTaskSensor or guessing at what time an upstream DAG "usually"
finishes.

Naming note: this feature is called Datasets in Airflow 2.4-2.x — this
workshop stack pins 2.9.3, so that is the name and the API used below.
Airflow 3.0 renamed the same concept to Assets, with an updated import path
(airflow.sdk.Asset). The mental model here still applies after an upgrade;
only the name and import change.
"""

from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# A Dataset is just a URI — a logical name, not a live connection. Airflow
# never connects to this address itself; it only tracks "was this URI marked
# updated." Reusing the real raw_events table name keeps the identity
# meaningful to anyone reading these DAGs later.
RAW_EVENTS = Dataset("postgres://workshop-db/raw_events")


@dag(
    dag_id="raw_events_producer_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # triggered manually in the demo; a real pipeline might
    # run this hourly via schedule="@hourly" instead — the dataset
    # mechanics below don't change either way.
    catchup=False,
    tags=["demo", "datasets"],
)
def raw_events_producer_demo():
    @task(outlets=[RAW_EVENTS])
    def write_raw_event():
        # A trivial, deterministic write — no external API — so the demo
        # never depends on network conditions in the room.
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "INSERT INTO raw_events (id, event_date, payload) "
            "VALUES (%(id)s, CURRENT_DATE, %(payload)s) "
            "ON CONFLICT (id, event_date) DO UPDATE SET payload = EXCLUDED.payload;",
            parameters={
                "id": f"datasets-demo-{datetime.utcnow().isoformat()}",
                "payload": '{"source": "datasets_demo"}',
            },
        )
        # outlets=[RAW_EVENTS] above is the whole mechanism: Airflow marks
        # the dataset "updated" the instant this task succeeds. Nothing else
        # in this function is dataset-specific.

    write_raw_event()


raw_events_producer_demo()


@dag(
    dag_id="raw_events_consumer_demo",
    start_date=datetime(2024, 1, 1),
    schedule=[RAW_EVENTS],  # scheduled BY the dataset, not by cron
    catchup=False,
    tags=["demo", "datasets"],
)
def raw_events_consumer_demo():
    @task
    def print_triggering_dataset_events(triggering_dataset_events=None):
        # Despite the parameter name, Airflow keys this dict by the dataset's
        # URI string, not a Dataset object — dataset here is already the URI.
        for dataset, events in triggering_dataset_events.items():
            for event in events:
                print(
                    f"Triggered by dataset '{dataset}', updated by DAG "
                    f"run '{event.source_dag_run.dag_id}' at "
                    f"{event.source_dag_run.data_interval_end}."
                )

    print_triggering_dataset_events()


raw_events_consumer_demo()
