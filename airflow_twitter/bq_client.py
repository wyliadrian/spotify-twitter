def create_bq_client():
    '''
    Authentication to grant access to BigQuery tables
    '''
    
    import os
    from google.cloud import bigquery

    access_key_path = "/home/airflow/gcs/dags/key-file.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = access_key_path
    
    bqclient = bigquery.Client().from_service_account_json(access_key_path)
    
    return bqclient
