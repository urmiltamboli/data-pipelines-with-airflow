from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Updated import for Airflow 2
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Updated import
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "urmiltamboli",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="final_project_dag",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    catchup=False,
)

start_operator = EmptyOperator(task_id="Begin_execution", dag=dag)

create_staging_events_table = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql",
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="ut-final",
    s3_key="log_data/{{ ds }}",
    json_path="s3://ut-final/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="ut-final",
    s3_key="song_data",
    json_path="auto",
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    append_only=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_only=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    append_only=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    append_only=False,
)

# ✅ Parameterized Data Quality Checks
dq_checks = [
    {"sql": "SELECT COUNT(*) FROM songplays", "expected": 1},
    {"sql": "SELECT COUNT(*) FROM users", "expected": 1},
    {"sql": "SELECT COUNT(*) FROM songs", "expected": 1},
    {"sql": "SELECT COUNT(*) FROM artists", "expected": 1},
    {"sql": "SELECT COUNT(*) FROM time", "expected": 1},
]

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tests=dq_checks,
)

end_operator = EmptyOperator(task_id="Stop_execution", dag=dag)

# ✅ DAG Dependencies
start_operator >> create_staging_events_table >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
