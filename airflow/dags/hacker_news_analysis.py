from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the database URL
DATABASE_URL = 'postgresql+psycopg2://admin:admin@localhost:5432/hack_news_analysis'

# Create a new SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define a base class for the declarative model
Base = declarative_base()

# Define the HackerNewsStory model
class HackerNewsStory(Base):
    __tablename__ = 'hacker_news_stories'
    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(Integer, unique=True, nullable=False)
    title = Column(String, nullable=False)

# Create the table if it does not exist
Base.metadata.create_all(engine)

# Create a new session factory
Session = sessionmaker(bind=engine)

def fetch_and_save_top_stories():
    # Fetch top stories from Hacker News
    top_stories_url = 'https://hacker-news.firebaseio.com/v0/topstories.json'
    top_stories_response = requests.get(top_stories_url)
    if top_stories_response.status_code != 200:
        raise Exception("Failed to fetch top stories")
    
    top_stories_ids = top_stories_response.json()[:10]
    stories = []
    
    for story_id in top_stories_ids:
        story_url = f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json'
        story_response = requests.get(story_url)
        
        if story_response.status_code == 200:
            story_data = story_response.json()
            title = story_data.get('title', '')
            stories.append(HackerNewsStory(story_id=story_id, title=title))
    
    # Insert stories into the PostgreSQL database
    session = Session()
    try:
        session.bulk_save_objects(stories)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    print("Stories saved to the PostgreSQL database")

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
    description='Fetch top stories from Hacker News and save to a PostgreSQL database using SQLAlchemy',
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
