from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, asset, dag, task


@asset(schedule="@daily")
def source_max_update_dt():
    hook = PostgresHook(postgres_conn_id="postgres_15")
    records = hook.get_records("SELECT MAX(updated_date) FROM source_table;")
    for rec in records:
        print(rec)
    return records


@asset(schedule=[Asset("source_max_update_dt")])
def sink_max_update_dt():
    hook = PostgresHook(postgres_conn_id="postgres_13")
    records = hook.get_records("SELECT MAX(updated_date) FROM sink_table;")
    for rec in records:
        print(rec)
    return records


@dag(schedule=(Asset("source_max_update_dt") & Asset("sink_max_update_dt")))
def after_postgres():
    @task
    def print_result(**context):
        source_max_update_dt_data = context["ti"].xcom_pull(
            dag_id="source_max_update_dt",
            task_ids="source_max_update_dt",
            key="return_value",
            include_prior_dates=True,
        )

        sink_max_update_dt_data = context["ti"].xcom_pull(
            dag_id="sink_max_update_dt",
            task_ids="sink_max_update_dt",
            key="return_value",
            include_prior_dates=True,
        )
        print(type(source_max_update_dt_data))
        print(source_max_update_dt_data[-1][0])
        print(type(sink_max_update_dt_data))
        print(sink_max_update_dt_data[-1][0])
        # source_max_update_dt = (
        #     source_max_update_dt_data[0][0].isoformat()
        #     if source_max_update_dt_data[0][0]
        #     else None
        # )
        # sink_max_update_dt = (
        #     sink_max_update_dt_data[0][0].isoformat()
        #     if sink_max_update_dt_data[0][0]
        #     else None
        # )
        # print(f"SOURCE UPDATED DT: {source_max_update_dt}")
        # print(f"SINK UPDATED DT: {sink_max_update_dt}")

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
