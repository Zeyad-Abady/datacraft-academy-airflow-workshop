from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Demo toggle: flip to True, save, and re-trigger to watch the failure path.
FORCE_FAILURE = False


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["trigger-rules-demo"],
)
def trigger_rules_demo():

    @task(retries=3, retry_delay=timedelta(seconds=10))
    def insert_row():
        # Same PostgresHook pattern as Lab 2. Flip FORCE_FAILURE above and
        # re-trigger to watch it fail after 3 retries.
        if FORCE_FAILURE:
            raise RuntimeError("Simulated failure for the trigger rules demo")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "INSERT INTO lab1_staging (id, text, loaded_at) "
            "VALUES (%s, %s, NOW()) ON CONFLICT (id) DO NOTHING;",
            parameters=("trigger-rules-demo", "inserted by trigger_rules_demo"),
        )

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def alert_on_any_failure():
        print("[ALERT] insert_row failed after all retries — page on-call here.")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def send_status_report():
        print("Run finished — this always executes, success or failure.")

    insert_row() >> [alert_on_any_failure(), send_status_report()]


trigger_rules_demo()
