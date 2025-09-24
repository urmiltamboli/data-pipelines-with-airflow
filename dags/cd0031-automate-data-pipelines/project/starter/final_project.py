from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'urmiltamboli',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    dag_id='final_project_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

start_operator = DummyOperator(task_id='Begin_execution')

create_staging_events_table = PostgresOperator(
task_id="Create_tables",
dag=dag,
postgres_conn_id="redshift",
sql='create_tables.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
task_id='Stage_events',
dag=dag,    
redshift_conn_id="redshift",
aws_credentials_id="aws_credentials",    
table = "staging_events",
s3_path = "s3://udacity-dend/log_data",
json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
task_id='Stage_songs',
dag=dag,  
redshift_conn_id="redshift",
aws_credentials_id="aws_credentials",    
table = "staging_songs",
s3_path = "s3://udacity-dend/song_data",
json_path="auto"
)

load_songplays_table = LoadFactOperator(
task_id='Load_songplays_fact_table',
dag=dag,
redshift_conn_id="redshift",
table='songplays',
sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    append_only=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift" ,
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_only=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift" ,
    table="artists",
    sql=SqlQueries.artist_table_insert,
    append_only=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    append_only=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# set task accordign to required flow
start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator