# from datetime import datetime, timedelta

# import pandas as pd
# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator
# from airflow.providers.mysql.hooks.mysql import MySqlHook


# def get_last_sync_value(table_name, tracking_column):
#     """Retrieve last synced value from Airflow Variables"""
#     key = f"{table_name}_{tracking_column}_last_sync"
#     return Variable.get(key, default_var=None)


# def set_last_sync_value(table_name, tracking_column, value):
#     """Store last synced value in Airflow Variables"""
#     key = f"{table_name}_{tracking_column}_last_sync"
#     Variable.set(key, str(value))


# def incremental_upsert(
#     source_conn_id,
#     sink_conn_id,
#     table_name,
#     primary_key,
#     tracking_column,
#     chunk_size=10000,
#     **context,
# ):
#     source_hook = MySqlHook(mysql_conn_id=source_conn_id)
#     sink_hook = MySqlHook(mysql_conn_id=sink_conn_id)

#     # Get last sync value
#     last_sync = get_last_sync_value(table_name, tracking_column)

#     # Build incremental query
#     if last_sync:
#         query = f"""
#         SELECT * FROM {table_name}
#         WHERE {tracking_column} > '{last_sync}'
#         ORDER BY {tracking_column}
#         """
#     else:
#         # First run - full load
#         query = f"SELECT * FROM {table_name}"

#     source_conn = source_hook.get_conn()
#     sink_conn = sink_hook.get_conn()
#     sink_cursor = sink_conn.cursor()

#     total_rows = 0
#     max_tracking_value = last_sync

#     # Process in chunks
#     for chunk in pd.read_sql(query, source_conn, chunksize=chunk_size):
#         if chunk.empty:
#             break

#         total_rows += len(chunk)

#         # Get column names
#         columns = chunk.columns.tolist()
#         placeholders = ", ".join(["%s"] * len(columns))
#         columns_str = ", ".join([f"`{col}`" for col in columns])

#         # Build upsert query (MySQL syntax)
#         update_clause = ", ".join(
#             [f"`{col}` = VALUES(`{col}`)" for col in columns if col != primary_key]
#         )

#         upsert_query = f"""
#         INSERT INTO {table_name} ({columns_str})
#         VALUES ({placeholders})
#         ON DUPLICATE KEY UPDATE {update_clause}
#         """

#         # Execute batch upsert
#         data = [tuple(row) for row in chunk.values]
#         sink_cursor.executemany(upsert_query, data)
#         sink_conn.commit()

#         # Track max value
#         chunk_max = chunk[tracking_column].max()
#         if max_tracking_value is None or chunk_max > max_tracking_value:
#             max_tracking_value = chunk_max

#     # Update last sync value
#     if max_tracking_value:
#         set_last_sync_value(table_name, tracking_column, max_tracking_value)

#     sink_cursor.close()
#     sink_conn.close()
#     source_conn.close()

#     print(f"Migrated {total_rows} rows. Last sync value: {max_tracking_value}")

#     return {"rows_migrated": total_rows, "last_sync_value": str(max_tracking_value)}


# with DAG(
#     dag_id="mysql_incremental_upsert_migration",
#     start_date=datetime(2024, 1, 1),
#     schedule="0 2 * * *",  # Daily at 2 AM
#     catchup=False,
#     max_active_runs=1,
#     default_args={
#         "retries": 2,
#         "retry_delay": timedelta(minutes=5),
#     },
# ) as dag:
#     migrate_task = PythonOperator(
#         task_id="incremental_upsert_migration",
#         python_callable=incremental_upsert,
#         op_kwargs={
#             "source_conn_id": "mysql_source",
#             "sink_conn_id": "mysql_sink",
#             "table_name": "your_table",
#             "primary_key": "id",  # Primary key column
#             "tracking_column": "updated_at",  # Change tracking column
#             "chunk_size": 10000,
#         },
#     )
