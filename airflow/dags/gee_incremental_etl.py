# gee_incremental_etl.py
from datetime import datetime,timedelta
from airflow.decorators import dag, task
from app.ee_physical import batch_process 
@dag(
    dag_id="ee_physical_pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="0 * * * *",      
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2), "owner": "data"},
    tags=["gee", "app"],
)
def processPixels():
    @task
    def task_run():
        batch_process()
        return {"status": "ok"}

    task_run()

dag = processPixels()