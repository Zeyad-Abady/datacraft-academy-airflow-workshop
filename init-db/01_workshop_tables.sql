-- Tables used by the Data Orchestration workshop labs.
-- Runs automatically on first Postgres boot (mounted into
-- /docker-entrypoint-initdb.d) so students never have to hand-write DDL
-- before they can start on the DAGs.

CREATE TABLE IF NOT EXISTS lab1_staging (
    id          TEXT PRIMARY KEY,
    text        TEXT NOT NULL,
    loaded_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_events (
    id              TEXT,
    event_date      DATE NOT NULL,
    payload         JSONB,
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, event_date)
);

CREATE TABLE IF NOT EXISTS clean_events (
    id              TEXT,
    event_date      DATE NOT NULL,
    payload         JSONB,
    cleaned_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, event_date)
);

CREATE TABLE IF NOT EXISTS quarantine_events (
    id              TEXT,
    event_date      DATE NOT NULL,
    payload         JSONB,
    reason          TEXT,
    quarantined_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reporting_daily (
    report_date     DATE PRIMARY KEY,
    event_count     INTEGER NOT NULL,
    generated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
