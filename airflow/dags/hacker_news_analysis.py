# dags/hacker_news_analysis.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
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

# Define the PythonOperator
fetch_stories_task = PythonOperator(
    task_id='fetch_and_save_top_stories',
    python_callable=fetch_and_save_top_stories,
    dag=dag,
)

# Define the task pipeline
fetch_stories_task
