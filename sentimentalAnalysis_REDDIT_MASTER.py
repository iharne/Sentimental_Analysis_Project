import sys
import os
import ast
import re
import pandas as pd
from sqlalchemy import create_engine
from nltk.sentiment import SentimentIntensityAnalyzer
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup

# Database connection parameters
db_url = "postgresql+psycopg2://iharne1:iharne1@localhost:5432/my_analytics"

# SQL query to fetch data
query = """
SELECT comments
FROM public."CHAN_MASTER" limit 1000;
"""

def fetch_and_analyze_data():
    try:
        # Create a database engine
        engine = create_engine(db_url)

        # Execute the SQL query and load data into a Pandas DataFrame
        df = pd.read_sql_query(query, engine)

        # Convert the string representation of list to an actual list
        df['comments'] = df['comments'].apply(ast.literal_eval)

        # Explode the list so each comment becomes a separate row
        df_exploded = df.explode('comments')

        # Updated Data Cleaning Function for Twitter Comments
        def clean_text_twitter(text):
            if pd.isna(text):
                return None

            # Remove HTML tags using BeautifulSoup
            text = BeautifulSoup(text, "html.parser").get_text()

            # Remove URLs
            text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)

            # Remove Mentions and Hashtags
            text = re.sub(r'@\w+|#\w+', '', text)

            # Remove special characters, numbers, and emojis
            text = re.sub(r'[^A-Za-z\s]', '', text)

            # Convert text to lowercase
            text = text.lower()

            # Remove stopwords
            stop_words = set(stopwords.words('english'))
            text = ' '.join([word for word in text.split() if word not in stop_words])

            # Lemmatization
            lemmatizer = WordNetLemmatizer()
            text = ' '.join([lemmatizer.lemmatize(word) for word in text.split()])

            return text

        # Apply text cleaning to each comment
        df_exploded['cleaned_comments'] = df_exploded['comments'].apply(clean_text_twitter)

        # Drop the original, uncleaned comments column
        df_exploded.drop('comments', axis=1, inplace=True)

        # Initialize sentiment analyzer
        sia = SentimentIntensityAnalyzer()

        # Define a function to categorize sentiment
        def categorize_sentiment(comment):
            if pd.isna(comment):  # Check for NaN values
                return None
            score = sia.polarity_scores(str(comment))['compound']
            if score >= 0.05:
                return 'Positive'
            elif score <= -0.05:
                return 'Negative'
            else:
                return 'Neutral'

        # Apply sentiment analysis to each cleaned comment
        df_exploded['sentiment'] = df_exploded['cleaned_comments'].apply(categorize_sentiment)

        # Convert the DataFrame to JSON
        json_data = df_exploded.to_json(orient='records')
        return json_data

    except Exception as e:
        print("An error occurred:", str(e))
        return {}

# If you want to test the function independently
if __name__ == '__main__':
    data = fetch_and_analyze_data()
    print(data)
