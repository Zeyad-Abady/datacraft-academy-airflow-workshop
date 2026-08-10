# Apache Airflow for Data Engineers — Workshop Stack

Local, Docker-only Airflow environment for the Data Craft "Apache Airflow for
Data Engineers" course (Sessions 1 & 2). No cloud accounts, no billing.

## Prerequisites

- **Docker Desktop** installed and running (this is the one thing you cannot
  do live in the session — it's a multi-GB download).
- **git**
- ~4 GB of free RAM for Docker

## Before Session 1 — Pre-Work Checklist (students, please do this ahead of time)

This takes about 15–20 minutes depending on your internet connection. Doing
it before Session 1 means we spend class time learning, not watching a
progress bar.

1. **Install Docker Desktop** (skip if already installed):
   - Windows: https://docs.docker.com/desktop/install/windows-install/
   - Mac: https://docs.docker.com/desktop/install/mac-install/
   - Linux: https://docs.docker.com/desktop/install/linux-install/
   - After installing, **open Docker Desktop and make sure it's running**
     (you should see the whale icon in your system tray / menu bar).
2. **Install Git** (skip if already installed): https://git-scm.com/downloads
3. **Clone and boot the stack** — open a terminal and run:

   ```bash
   git clone https://github.com/Zeyad-Abady/datacraft-academy-airflow-workshop.git
   cd datacraft-academy-airflow-workshop
   cp .env.sample .env

   # Linux only — avoids file permission issues on mounted volumes:
   # echo "AIRFLOW_UID=$(id -u)" >> .env

   docker compose build          # builds the custom image (requests pre-installed)
   docker compose up airflow-init   # one-time: migrates the DB, creates the admin user
   docker compose up -d          # boots postgres + webserver + scheduler
   docker compose ps             # confirm everything is healthy
   ```

4. **Confirm it worked:**
   - `docker compose ps` shows `postgres`, `airflow-webserver`, and
     `airflow-scheduler` all healthy / up.
   - http://localhost:8080 loads in your browser and you can log in with
     `airflow` / `airflow`.
5. **Shut it down until session day** (optional, but recommended if you're
   short on laptop battery/RAM):

   ```bash
   docker compose down
   ```

   This keeps everything you downloaded cached — on the day, bringing it
   back up is just `docker compose up -d` and takes seconds, not minutes.

Stuck on any step? See Troubleshooting below, or message the instructor
*before* the session so we can sort it out ahead of time.

### Instructor note

`docker compose build` and the first `docker compose up -d` pull ~1 GB of
images and build a custom image — this can take several minutes on
conference wifi. Run these commands the night before (or first thing when
you arrive on-site) so the live "boot the stack" demo in Session 1 only
needs `docker compose up -d`, which comes up in seconds once images are
cached locally.

## Tear Down

```bash
docker compose down       # stop everything, keep data
docker compose down -v    # stop everything AND wipe the database (fresh start)
```

## Folder Structure

```
airflow-workshop/
├── docker-compose.yaml   # the whole stack: postgres, webserver, scheduler
├── Dockerfile            # apache/airflow base + requests
├── dags/                 # DROP YOUR WORKING DAGS HERE — this is what Airflow scans
├── labs/                 # starter files with TODOs — copy into dags/ to begin a lab
├── solutions/            # full reference implementations for each lab
├── sql/lab3/             # SQL files used by the Lab 3 capstone's SQLExecuteQueryOperator
└── init-db/              # SQL that auto-creates the workshop's Postgres tables on first boot
```

## Labs

| Lab | Starter | Solution | Covered in |
|---|---|---|---|
| Lab 1 — Your First DAG | `labs/lab1_starter.py` | `solutions/lab1_first_dag.py` | Session 1 (live-coded) |
| Lab 2 — Strengthen It | `labs/lab2_starter.py` | `solutions/lab2_strengthen_it.py` | Session 1 (in-class, 6 min) |
| Lab 3 — Capstone Pipeline | `labs/lab3_starter.py` | `solutions/lab3_capstone_pipeline.py` | Session 2 |

To work a lab: copy the starter file from `labs/` into `dags/`, fill in the
`TODO`s, save, and it will appear in the Airflow UI within ~30 seconds. If
you get stuck, the matching file in `solutions/` is the full answer — try
not to peek until you've had a real attempt.

### Optional Post-Session Practice

Not a numbered lab, so it's not required — but the branching pattern here is
exactly what Lab 3's capstone needs, so doing this between Session 1 and
Session 2 makes the capstone noticeably easier.

| Task | Starter | Solution | When |
|---|---|---|---|
| Add a Quality Check | `labs/quality_check_practice.py` | `solutions/quality_check_practice.py` | Between Session 1 and 2 (~1 hr, optional) |

## Pre-Created Postgres Tables

`init-db/01_workshop_tables.sql` runs automatically the first time the
`postgres` container boots (fresh volume) and creates:

- `lab1_staging` — Lab 1 target table
- `raw_events`, `clean_events`, `quarantine_events`, `reporting_daily` — Lab 3
  capstone's Bronze/Silver/Gold-style tables

You do not need to write any `CREATE TABLE` statements yourself — just
`INSERT`/`SELECT` against these.

## Troubleshooting

- **Port 8080 already in use** — another app or a leftover container is
  using it. Run `docker compose down`, confirm nothing else is bound to
  8080, then retry. Or change the port mapping in `docker-compose.yaml`
  (`"8080:8080"` → `"8081:8080"`) and use that port instead.
- **`docker compose ps` shows a service unhealthy** — give it another 30–60
  seconds on first boot; `airflow-webserver` and `airflow-scheduler` both
  wait on `airflow-init` to finish first.
- **Permission errors on mounted volumes (Linux)** — make sure `AIRFLOW_UID`
  in `.env` matches your host user id: `echo "AIRFLOW_UID=$(id -u)" >> .env`,
  then `docker compose down && docker compose up -d`.
- **Want a totally fresh start** — `docker compose down -v` wipes the
  Postgres volume (including workshop tables), which get recreated from
  `init-db/` on the next `docker compose up -d`.

## Airflow Connection Used by the Labs

Set up once, live, in Session 1 (Admin → Connections → +):

| Field | Value |
|---|---|
| Connection Id | `postgres_default` |
| Connection Type | `Postgres` |
| Host | `postgres` |
| Schema | `airflow` |
| Login | `airflow` |
| Password | `airflow` |
| Port | `5432` |
