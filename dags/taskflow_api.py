from airflow.decorators import dag, task
from datetime import datetime
 
@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def first_taskflow_dag():
    @task
    def extract():
        return {"rows": 42}
 
    @task
    def transform(data):
        print(f"processing {data['rows']} rows")
 
    transform(extract())
 
first_taskflow_dag()
