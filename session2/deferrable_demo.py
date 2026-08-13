"""
Instructor demo — Sensors in Practice: poke vs. reschedule vs. deferred
=========================================================================
Session 2, "Sensors in Practice" slide (right after the Sensors concept
slide). Not a student lab — copy this into dags/ and run it live.

One DAG, three tasks — same FileSensor, same signal file, three different
modes. Triggering the DAG once starts all three in parallel, so you can
compare their states side by side in a single Graph/Grid View.

Requires the airflow-triggerer container (added in docker-compose.yaml
specifically for this demo). Confirm it's up first:

    docker compose ps   # should show 4 healthy services, incl. airflow-triggerer

Run of show
-----------
1. Copy this file into dags/ if it isn't already synced there, and confirm
   the sensor_modes_demo DAG appears, unpaused, with three tasks:
     - wait_for_file_poke
     - wait_for_file_reschedule
     - wait_for_file_deferred
2. Trigger the DAG once.
3. In the Grid View, watch the three task rows for this one run:
     - poke        -> one long, solid "running" bar for the whole wait.
     - reschedule  -> short "running" blips separated by "up_for_reschedule".
     - deferred    -> flips to "deferred" almost immediately and stays there.
4. Prove it at the process level, in a terminal. The airflow image does not
   ship procps, so there is no `ps` — use psutil instead (Airflow depends on
   it, so it's always there):

       docker compose exec airflow-scheduler python -c "import psutil; \
       [print(p.pid, ' '.join(p.cmdline())) for p in psutil.process_iter() \
       if any('airflow' in a for a in p.cmdline())]"

   During the wait, the poke task shows a live `airflow tasks run` child
   process the entire time; the reschedule task's process appears and
   disappears every poke_interval; the deferred task has NO scheduler/worker
   process at all. Instead, run the same command against airflow-triggerer:

       docker compose exec airflow-triggerer python -c "import psutil; \
       [print(p.pid, ' '.join(p.cmdline())) for p in psutil.process_iter() \
       if any('airflow' in a for a in p.cmdline())]"

   shows a single triggerer process — that one process is holding the async
   wait for the deferred task (and would hold thousands more, the same way).
5. Unblock all three at once from the HOST terminal (not docker exec —
   ./dags is bind-mounted, so a plain host-side file write is visible inside
   every container):

       touch dags/.go_signal                                    # macOS/Linux
       New-Item -ItemType File -Force -Path dags\.go_signal      # Windows PowerShell

   All three tasks should succeed within one poke_interval / one triggerer
   tick (~10 seconds here).
6. Clean up before the next run:

       rm dags/.go_signal            # macOS/Linux
       Remove-Item dags\.go_signal   # Windows PowerShell (rm is aliased too)

The punchline: poke wastes a worker slot for the entire wait. reschedule
frees the slot between checks but still costs a scheduler/worker cycle every
poke_interval. deferred costs zero worker capacity for the whole wait — one
triggerer process can hold thousands of these at once. This is exactly the
architecture distinction from Session 1 (webserver / scheduler / executor /
workers / metadata DB, plus the triggerer foreshadowed back then) paying off
in a real production concern: a 6-hour poke-mode sensor with 4 workers
configured is 25% of your capacity gone for 6 hours, for nothing.

Note: FileSensor resolves `filepath` against the `fs_default` Connection,
which ships as a default Airflow connection (type "File (Path)", empty base
path). If it's missing in your environment, add it once: Admin -> Connections
-> +, Connection Id `fs_default`, Connection Type `File (path)`, leave Path
blank.
"""

from datetime import datetime

from airflow.decorators import dag
from airflow.sensors.filesystem import FileSensor

SIGNAL_PATH = "/opt/airflow/dags/.go_signal"


@dag(
    dag_id="sensor_modes_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "sensors", "deferrable"],
)
def sensor_modes_demo():
    FileSensor(
        task_id="wait_for_file_poke",
        filepath=SIGNAL_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 20,
        mode="poke",  # holds a worker slot for the entire wait
    )

    FileSensor(
        task_id="wait_for_file_reschedule",
        filepath=SIGNAL_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 20,
        mode="reschedule",  # frees the worker slot between checks
    )

    FileSensor(
        task_id="wait_for_file_deferred",
        filepath=SIGNAL_PATH,
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 20,
        deferrable=True,  # hands off to the Triggerer — zero worker cost while waiting
    )
    # No dependencies between the three — they run in parallel from the same
    # trigger, which is exactly what makes the side-by-side comparison work.


sensor_modes_demo()
