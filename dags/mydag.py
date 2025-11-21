from airflow.sdk import dag, task
from pendulum import datetime


@dag(start_date=datetime(2025, 4, 22), schedule="@daily", tags=["mydag"])
def my_dag():
    @task()
    def my_dag_task(**context) -> str:
        print("####$@$#@. my dag task")

    my_dag_task()


my_dag()
