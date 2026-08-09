FROM apache/airflow:2.9.3

# Extra Python deps used by the workshop labs (baked in at build time so
# `docker compose up -d` doesn't reinstall packages on every boot).
RUN pip install --no-cache-dir requests==2.32.3
