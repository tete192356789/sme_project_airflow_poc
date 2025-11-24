import datetime
import logging

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
        both_dt = context["ti"].xcom_pull(
            dag_id="date_comparison_dag",
            task_ids="get_date_from_both",
            key="return_value",
            include_prior_dates=True,
        )

        logger.info(both_dt)

    get_date_from_both() >> compare_date()


date_comparison_dag()
