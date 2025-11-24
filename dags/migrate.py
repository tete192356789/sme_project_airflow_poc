from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, asset, dag, task


@asset(schedule="@daily")
def postgres_asset():
    hook = PostgresHook(postgres_conn_id="postgres_15")
    records = hook.get_records("SELECT * FROM source_table;")
    for rec in records:
        print(rec)
    return records


@dag(schedule=[Asset("postgres_asset")])
def after_postgres():
    @task
    def print_result(**context):
        data = context["ti"].xcom_pull(
            dag_id="postgres_asset",
            task_ids="postgres_asset",
            key="return_value",
            include_prior_dates=True,
        )
        print("###################")
        print(data[0])

    print_result()


after_postgres()
# @asset(schedule="@daily")
# def extracted_data():
#     return {"a": 1, "b": 2}
# @asset(schedule=extracted_data)
# def transformed_data(context):
#     data = context["ti"].xcom_pull(
#         dag_id="extracted_data",
#         task_ids="extracted_data",
#         key="return_value",
#         include_prior_dates=True,
#     )
#     return {k: v * 2 for k, v in data.items()}
