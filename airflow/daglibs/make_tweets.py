# !pip install --upgrade 'google-cloud-bigquery[bqstorage,pandas]'
from datetime import datetime, timedelta
from daglibs import bq_client

def make_tweet(**context):
    
    # Set necesary information
    PROJECT_ID = "graphic-segment-355021"
    DATASET_ID = "spotify_data"
    TABLE_NAME_1 = "albums"
    TABLE_NAME_2 = "tweets"

    TABLE_ID_1 = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_1}"
    TABLE_ID_2 = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME_2}"   
    
    # Initialize the client object to interact with BigQuery
    bqclient = bq_client.create_bq_client()
    
    # Determine which certain row to retrieve
    start_time = context["templates_dict"]["start_date"]
    time_diff = (datetime.now() - start_time).total_seconds()/60
    row_num = int((time_diff/3)-1)%15
    
    # SQL query
    query_string = f"""
    SELECT *
    FROM {TABLE_ID_1}
    ORDER BY release_date
    LIMIT 1 OFFSET {row_num}
    """
    
    # Query job against BigQuery table and transform the results into a data frame
    df = (bqclient.query(query_string).result().to_dataframe(create_bqstorage_client=True))
    
    # Obtain certain fields and prepare the rows to be inserted into the table
    rd = df.loc[0, "release_date"]
    an = df.loc[0, "album_name"]
    au = df.loc[0, "album_url"]   
    tweet_post = f"On {rd}, #MariahCarey released her album '{an}'. Check out on Spotify:{au}."
    row_to_insert = [{"release_date": rd, "text": tweet_post}]
    
    # Perform insertion
    bqclient.insert_rows_json(TABLE_ID_2, row_to_insert)