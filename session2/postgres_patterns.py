from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="postgres_patterns_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "postgres"],
    # sql/ is its own top-level Docker volume (a sibling of dags/, not
    # nested under it — see docker-compose.yaml), so a relative sql=
    # path below only resolves once this is set.
    template_searchpath=["/opt/airflow/sql"],
)
def postgres_patterns_demo():
    @task
    def clear_todays_clean_rows(ds=None):
        # PostgresHook — Python-driven. ds is injected automatically because
        # the parameter is named after a context variable; Jinja does NOT
        # render strings inside a task body on its own.
        hook = PostgresHook(postgres_conn_id="postgres_default")
        hook.run(
            "DELETE FROM clean_events WHERE event_date = %(d)s;",
            parameters={"d": ds},
        )

    # SQLExecuteQueryOperator — declared SQL, no Python logic at all.
    # Reads sql/lab3/upsert_clean.sql (already in the repo). `sql` and
    # `parameters` ARE templated fields on this operator, so "{{ ds }}"
    # really does get Jinja-rendered before Postgres ever sees it.
    upsert_clean = SQLExecuteQueryOperator(
        task_id="upsert_clean",
        conn_id="postgres_default",
        sql="lab3/upsert_clean.sql",
        parameters={"run_date": "{{ ds }}"},
    )

    clear_todays_clean_rows() >> upsert_clean


postgres_patterns_demo()
