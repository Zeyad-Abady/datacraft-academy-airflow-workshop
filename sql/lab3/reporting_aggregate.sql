-- Idempotent aggregation: clean_events -> reporting_daily for a single run date.

INSERT INTO reporting_daily (report_date, event_count, generated_at)
SELECT
    event_date,
    COUNT(*),
    NOW()
FROM clean_events
WHERE event_date = %(run_date)s::date
GROUP BY event_date
ON CONFLICT (report_date) DO UPDATE
    SET event_count  = EXCLUDED.event_count,
        generated_at = EXCLUDED.generated_at;
