from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import json
from datetime import datetime as dt

def fetch_and_save_top_stories():
    # Fetch top stories from Hacker News
    top_stories_url = 'https://hacker-news.firebaseio.com/v0/topstories.json'
    top_stories_response = requests.get(top_stories_url)
    if top_stories_response.status_code != 200:
        raise Exception("Failed to fetch top stories")
    
    top_stories_ids = top_stories_response.json()[:50]
    stories = []
    
    for story_id in top_stories_ids:
        story_url = f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json'
        story_response = requests.get(story_url)
        
        if story_response.status_code == 200:
            story_data = story_response.json()
            title = story_data.get('title', '')
            stories.append({
                'id': story_id,
                'title': title
            })
    
    # Specify the directory to save the file
    directory = "/Users/johnpherson/Documents/github_repo/airflow_projects/json_results"
    
    # Ensure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with a timestamp
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(directory, f"top_stories_{timestamp}.json")
    
    # Save stories to a JSON file
    with open(filename, 'w') as f:
        json.dump(stories, f, indent=4)
    print(f"Stories saved to {filename}")

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
    description='Fetch top stories from Hacker News and save to a JSON file',
    schedule_interval=timedelta(days=1),  # Schedule to run once a day
    start_date=datetime(2023, 1, 1),
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