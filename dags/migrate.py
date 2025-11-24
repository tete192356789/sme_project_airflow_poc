import datetime
import logging

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, asset, dag, task

logger = logging.getLogger(__name__)


@asset(schedule="@daily")
def source_max_update_dt():
    hook = PostgresHook(postgres_conn_id="postgres_15")
    logger.info("Getting Max Updated Date From Source Table.")
    records = hook.get_records("SELECT MAX(updated_date) FROM source_table;")
    logger.info(f"Source Table Max Updated Date: {records[0][0]}")

    return records


@asset(schedule=[Asset("source_max_update_dt")])
def sink_max_update_dt():
    hook = PostgresHook(postgres_conn_id="postgres_13")
    logger.info("Getting Max Updated Date From Sink Table.")
    records = hook.get_records("SELECT MAX(updated_date) FROM sink_table;")
    logger.info(f"Sink Table Max Updated Date: {records[0][0]}")
    return records


@dag(schedule=(Asset("source_max_update_dt") & Asset("sink_max_update_dt")))
def date_comparison_dag():
    @task
    def get_date_from_both(**context):
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

        source_max_update_dt = (
            source_max_update_dt_data[-1][0][0].isoformat()
            if (source_max_update_dt_data[-1][0][0], datetime.datetime)
            else None
        )
        sink_max_update_dt = (
            sink_max_update_dt_data[-1][0][0].isoformat()
            if isinstance(sink_max_update_dt_data[-1][0][0], datetime.datetime)
            else None
        )
        logger.info(f"SOURCE UPDATED DT: {source_max_update_dt}")
        logger.info(f"SINK UPDATED DT: {sink_max_update_dt}")
        return {"source_dt": source_max_update_dt, "sink_dt": sink_max_update_dt}

    @task.branch
    def compare_date(**context):
        date_data = context["ti"].xcom_pull(
            dag_id="date_comparison_dag",
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )
        if not date_data[0]["sink_dt"]:
            logger.info("Sink date is None. Updating!!!")
            return "update_task"
        if not isinstance(
            datetime.datetime.fromisoformat(date_data[0]["sink_dt"]), datetime.datetime
        ):
            logger.info("Sink date is not type datetime. Updating!!!")
            return "update_task"
        if date_data[0]["source_dt"] > date_data[0]["sink_dt"]:
            logger.info("Source Date More Than Sink Date. Updating!!!")
            return "update_task"

        return "not_update_task"

    @task(outlets=[Asset("update_asset")])
    def update_task(**context):
        logger.info("Updating update_asset !!!")

    @task(outlets=[Asset("not_update_asset")])
    def not_update_task(**context):
        logger.info("Updating not_update_asset !!!")

    get_date_from_both() >> compare_date() >> [update_task(), not_update_task()]


@dag(dag_id="incremental_update", schedule=[Asset("update_asset")])
def incremental_update():
    @task
    def get_source_data(**context):
        date_data = context["ti"].xcom_pull(
            dag_id="date_comparison_dag",
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )
        logger.info(date_data)

        # Build incremental query
        if date_data[-1]["sink_data"]:
            query = f"""
            SELECT * FROM source_table
            WHERE updated_date > '{date_data[-1]["sink_data"]}'
            ORDER BY source_table
            """
        else:
            # First run - full load
            query = "SELECT * FROM source_table"

        # source_conn = source_hook.get_conn()
        # sink_conn = sink_hook.get_conn()
        # sink_cursor = sink_conn.cursor()
        source_conn = PostgresHook(postgres_conn_id="postgres_15").get_conn()
        sink_conn = PostgresHook(postgres_conn_id="postgres_13").get_conn()
        sink_cursor = sink_conn.cursor()
        total_rows = 0
        primary_key = "id"

        # max_tracking_value = last_sync

        # Process in chunks
        for chunk in pd.read_sql(query, source_conn, chunksize=10000):
            if chunk.empty:
                break

            total_rows += len(chunk)

            # Get column names
            columns = chunk.columns.tolist()
            placeholders = ", ".join(["%s"] * len(columns))
            columns_str = ", ".join([f"`{col}`" for col in columns])

            # Build upsert query (MySQL syntax)
            update_clause = ", ".join(
                [f"`{col}` = VALUES(`{col}`)" for col in columns if col != primary_key]
            )

            upsert_query = f"""
            INSERT INTO sink_table ({columns_str})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
            """

            # Execute batch upsert
            data = [tuple(row) for row in chunk.values]
            # sink_cursor.executemany(upsert_query, data)
            # sink_conn.commit()

            # Track max value
            # chunk_max = chunk[tracking_column].max()
            # if max_tracking_value is None or chunk_max > max_tracking_value:
            #     max_tracking_value = chunk_max
            logger.info(upsert_query)

    get_source_data()


date_comparison_dag()
incremental_update()
