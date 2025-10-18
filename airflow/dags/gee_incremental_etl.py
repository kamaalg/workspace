# gee_incremental_etl.py
from datetime import datetime
from airflow.decorators import dag, task

@dag(dag_id="hello_kamal", start_date=datetime(2025, 10, 1), schedule=None, catchup=False)
def hello_kamal():
    @task
    def hi():
        print("it works!")
    hi()

dag = hello_kamal()
