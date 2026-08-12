from datetime import datetime

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.decorators import dag


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def sql_sensor_demo():
    wait_for_raw = SqlSensor(
        task_id="wait_for_raw",
        conn_id="postgres_default",
        sql="SELECT 1 FROM raw_events WHERE event_date= '{{ ds }}' LIMIT 1",
        mode="reschedule",  # do NOT block a worker
        poke_interval=60,  # check once per minute
        timeout=60 * 6,
    )


sql_sensor_demo()
