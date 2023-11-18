from sqlalchemy import create_engine
import pandas as pd
import os
import ast
from nltk.sentiment import SentimentIntensityAnalyzer

# Database connection parameters
db_url = "postgresql+psycopg2://iharne1:iharne1@localhost:5432/my_analytics"

# SQL query to fetch data
query = """
SELECT comments
FROM public."REDDIT_MASTER" limit 1000;
"""

try:
    # Create a database engine
    engine = create_engine(db_url)

    # Execute the SQL query and load data into a Pandas DataFrame
    df = pd.read_sql_query(query, engine)

    # Convert the string representation of list to an actual list
    df['comments'] = df['comments'].apply(ast.literal_eval)

    # Explode the list so each comment becomes a separate row
    df_exploded = df.explode('comments')

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

    # Apply sentiment analysis to each comment
    df_exploded['sentiment'] = df_exploded['comments'].apply(categorize_sentiment)

    # Specify the full file path for the Excel file
    script_directory = os.path.dirname(os.path.abspath(__file__))
    excel_file_path = os.path.join(script_directory, "output.xlsx")

    # Export the data to an Excel file
    df_exploded.to_excel(excel_file_path, index=False)
    print("Data exported to Excel successfully.")

except Exception as e:
    print("An error occurred:", str(e))
