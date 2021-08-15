from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
                                
from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#S3 bucket addresses for log/song files
s3_log_bucket = "s3://udacity-dend/log_data"
s3_song_bucket = "s3://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json"
LOG_JSONPATH= "s3://udacity-dend/log_json_path.json"

#Default arguments for DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False,
    
}
#Setting DAG
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )
#Initial task for defining job's start
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#Copy Log Data from S3 buckets to redshift stage_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context = True,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    json_format = LOG_JSONPATH,
    s3_bucket = s3_log_bucket
    
)

#Copy Song Data from S3 buckets to redshift stage_songs table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context = True,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table = "staging_songs",
    json_format = "auto",
    s3_bucket = s3_song_bucket
)

#Insert Data from stage_events/song_events tables to songplays table in redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table = "songplays",
    query = SqlQueries.songplay_table_insert

)
#Insert Data from stage_events/song_events tables to users table in redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    delete_mode = True,
    table = "users",
    query = SqlQueries.user_table_insert
)

#Insert Data from stage_events/song_events tables to songs table in redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    delete_mode = True,
    table = "songs",
    query = SqlQueries.song_table_insert
)

#Insert Data from stage_events/song_events tables to artists table in redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    delete_mode = True,
    table = "artists",
    query = SqlQueries.artist_table_insert
)

#Insert Data from stage_events/song_events tables to time table in redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    delete_mode = True,
    table = "time",
    query = SqlQueries.time_table_insert
)

# Data quality check for all tables in Redshift
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables = ["staging_events","staging_songs","songplays","users","songs","artists","time"]
)

# Last task in pipeline for defining job's end
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Creating consequent execution steps for Airflow
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table>> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table>> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

