#!/usr/bin/env python
# coding: utf-8

# !pip install tweepy

import os
from datetime import datetime, timedelta
import tweepy
from daglibs import bq_client

def post_tweet():

    def get_tweet():

        PROJECT_ID = "graphic-segment-355021"
        DATASET_ID = "spotify_data"
        TABLE_NAME = "tweets"  
        TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        
        bqclient = bq_client.create_bq_client()
        
        query_string = f"""
        SELECT text
        FROM {TABLE_ID}
        ORDER BY release_date DESC
        LIMIT 1
        """
        
        df = bqclient.query(query_string).result().to_dataframe()
        tweet = df.loc[0, "text"]
        
        return tweet

    tweet = get_tweet()
    
    BEARER_TOKEN = "******"
    CONSUMER_KEY = "******"
    CONSUMER_SECRET = "******"
    ACCESS_TOKEN = "******"
    ACCESS_TOKEN_SECRET = "******"
    
    api = tweepy.Client(bearer_token=BEARER_TOKEN, 
                    access_token=ACCESS_TOKEN,
                    access_token_secret=ACCESS_TOKEN_SECRET,
                    consumer_key=CONSUMER_KEY,
                    consumer_secret=CONSUMER_SECRET)
    
    new_status = api.create_tweet(text = tweet)
