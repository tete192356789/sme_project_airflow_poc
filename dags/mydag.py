from datetime import datetime, timedelta

from airflow.sdk import dag, task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(seconds=30),
    tags=["mydag"],
    default_args=default_args,
)
def my_dag():
    @task()
    def my_dag_task(**context) -> str:
        print("####$@$#@. my dag task")

    my_dag_task()


my_dag()
