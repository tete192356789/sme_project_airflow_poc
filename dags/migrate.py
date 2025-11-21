from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, task


@dag
def my_producer_dag():
    @task(outlets=[Asset("my_asset")])
    def my_producer_task():
        print("Updating Asset: my_asset")

    my_producer_task()


my_producer_dag()


@dag(
    schedule=[Asset("my_asset")],
)
def my_consumer_dag():
    EmptyOperator(task_id="empty_task")


my_consumer_dag()
