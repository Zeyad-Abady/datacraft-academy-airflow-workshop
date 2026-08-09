-- Idempotent transform: raw_events -> clean_events for a single run date.
-- Safe to re-run for the same %(run_date)s any number of times.

INSERT INTO clean_events (id, event_date, payload, cleaned_at)
SELECT
    id,
    event_date,
    payload,
    NOW()
FROM raw_events
WHERE event_date = %(run_date)s::date
  AND payload IS NOT NULL
ON CONFLICT (id, event_date) DO UPDATE
    SET payload    = EXCLUDED.payload,
        cleaned_at = EXCLUDED.cleaned_at;
