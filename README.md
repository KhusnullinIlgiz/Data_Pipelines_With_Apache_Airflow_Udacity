# Data Pipelines with Airflow Udacity

## Goal of this project :blush: 
Goal of this project is to create an ETL process to extract, transform and load data from existing JSON log and song files from S3 bucket to AWS Redshift Datawarehouse using Apache Airflow to automate as much as possible ETL process. Star Schema of tables in Redshift allows quick analysys of data by using simple queries without JOIN statements. Apache Airflow allows to schedule tasks and see the execution status of Pipeline steps.


# Projects Pipeline

![Alt text](./Pipeline.png?raw=true "Title")
Apache Airflow Pipeline Graph representation is shown above. In this representation can be clearly seen consequent execution steps. Begin_execution step is defining the start of the Pipeline. Stage_songs and Stage_events are the steps, where Log/Song JSON files from S3 bucket are copied to stage_song/stage_events tables in Redshift, using AWS account credentials(access_key and secret_access) and AWS Redshift credentials(endpoint url, port number, Redshift password, aws user) to make a connection to these services from Apache Airflow. Load_tablename steps are used to insert data from stage table to defined star schema tables in Redshift, by using Redshift credentials in Apache Airflow. After Data is loaded, data quality check step is run, to see if all steps run as expected. By Stop_execution step we define the end of the execution of the Pipeline. UI of the Pipeline Grapf let us see the status of each step and proceed, if it is needed, to each of them to check logs by clicking on the name of the step.   


# Datasets

## Song Dataset

This dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).
Example of JSON file in data/songs directory:

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

## Log Dataset

This dataset represents activities of users which are then stored in JSON log files.
Example of JSON file in data/logs directory:

{"artist":null,"auth":"Logged In","firstName":"Adler","gender":"M","itemInSession":0,"lastName":"Barrera","length":null,"level":"free","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"GET","page":"Home","registration":1540835983796.0,"sessionId":248,"song":null,"status":200,"ts":1541470364796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.78.2 (KHTML, like Gecko) Version\/7.0.6 Safari\/537.78.2\"","userId":"100"}


# Project Files Structure Description

1. **create_tables.cfg** - contains sql statements to create tables in Redshift if they are not yet created 

2. **udac_example_dag.py** - describes all the consequent steps of pipeline, which will be a sourse file for Apache Airflow UI

3. **sql_queries.py** - contains sql INSERT statements for all tables in Redshift

4. **stage_redshift.py** - contains StageToRedshiftOperator class for reusablity purposes for COPY from S3 to Redshift operation

5. **load_fact.py** - contains LoadFactOperator class for reusablity purposes for INSERT statetements of Fact tables in Redshift operation

6. **load_dimention.py** - contains LoadDimentionOperator class for reusablity purposes for INSERT statetements of Dimention tables in Redshift operation

7. **load_fact.py** - contains DataQualityOperator class for reusablity purposes for checking the data quality of inserted tables

# Used Services and Tools

> - **S3**
> - **Redshift**
> - **Python 3.6**
> - **Apache Airflow**



