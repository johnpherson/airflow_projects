# dags/hacker_news_analysis.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import sys
import os
from pathlib import Path

# Set the no_proxy environment variable to bypass the proxy for all requests
os.environ["no_proxy"]="*"

# Add the scripts directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))

# Import the function from the target script
from fetch_top_stories import fetch_and_save_top_stories

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'fetch_hacker_news_stories',
    default_args=default_args,
    description='Fetch top stories from Hacker News and save to a PostgreSQL database',
    schedule_interval=timedelta(days=1),  # Schedule to run once a day
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


# Define the SQL file paths using Path module for better path handling
dag_folder = Path(__file__).parent
merge_sql_file_path = dag_folder.parent / 'sql' / 'merge_stg_data.sql'
create_view_sql_file_path = dag_folder.parent / 'sql' / 'create_view.sql'

# Read the SQL files
try:
    with open(merge_sql_file_path, 'r') as merge_file:
        merge_sql = merge_file.read()
except FileNotFoundError:
    raise FileNotFoundError(f"SQL file not found: {merge_sql_file_path}")

try:
    with open(create_view_sql_file_path, 'r') as view_file:
        create_view_sql = view_file.read()
except FileNotFoundError:
    raise FileNotFoundError(f"SQL file not found: {create_view_sql_file_path}")

# Define the PythonOperator task to fetch and save stories
fetch_stories_task = PythonOperator(
    task_id='fetch_stories_task',
    python_callable=fetch_and_save_top_stories,
    dag=dag,
)

# Define the PostgresOperator task to execute the merge SQL
merge_data_task = PostgresOperator(
    task_id='merge_data_task',
    postgres_conn_id='postgres_conn_id',  # Use the connection ID you set up in the Airflow UI
    sql=merge_sql,
    dag=dag,
)

# Define the PostgresOperator task to create the view
create_view_task = PostgresOperator(
    task_id='create_view_task',
    postgres_conn_id='postgres_conn_id',  # Use the connection ID you set up in the Airflow UI
    sql=create_view_sql,
    dag=dag,
)

# Define the task pipeline
fetch_stories_task >> merge_data_task >> create_view_task