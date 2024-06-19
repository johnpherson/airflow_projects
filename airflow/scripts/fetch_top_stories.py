# scripts/fetch_top_stories.py

import requests
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, exists, func
from sqlalchemy.orm import declarative_base, sessionmaker

# Define the database URL
DATABASE_URL = 'postgresql+psycopg2://login:pw@host:1234/db'

# Create a new SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define a base class for the declarative model
Base = declarative_base()

# Define the HackerNewsStory model
class HackerNewsStory(Base):
    __tablename__ = 'hacker_news_stories_stg'
    id = Column(Integer, primary_key=True, autoincrement=True)
    story_id = Column(Integer, unique=True, nullable=False)
    title = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

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
    
    session = Session()
    try:
        for story_id in top_stories_ids:
            # Check if the story already exists
            exists_query = session.query(exists().where(HackerNewsStory.story_id == story_id)).scalar()
            if not exists_query:
                story_url = f'https://hacker-news.firebaseio.com/v0/item/{story_id}.json'
                story_response = requests.get(story_url)
                
                if story_response.status_code == 200:
                    story_data = story_response.json()
                    title = story_data.get('title', '')
                    stories.append(HackerNewsStory(story_id=story_id, title=title))
        
        if stories:
            session.bulk_save_objects(stories)
            session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    print("Stories saved to the PostgreSQL database")
    print(stories)

if __name__ == "__main__":
    fetch_and_save_top_stories()
