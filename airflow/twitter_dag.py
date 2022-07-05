from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from daglibs import make_tweets
from daglibs import post_tweets
import sys
import os

default_args = {
    'owner': 'weiyi',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 5, 23, 20),
    'end_date': datetime(2022, 7, 5, 23, 52),
    'email': ['wyliadrian18@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
 }

dag = DAG('PostTweet_v4', schedule_interval = '*/2 * * * *', default_args = default_args)

t1 = PythonOperator(
    task_id = 'make_tweets',
    python_callable = make_tweets.make_tweet,
    templates_dict = default_args,
    provide_context = True,
    dag = dag)

t2 = PythonOperator(
    task_id = 'post_tweets',
    python_callable = post_tweets.post_tweet,
    dag = dag)

t1.set_downstream(t2)