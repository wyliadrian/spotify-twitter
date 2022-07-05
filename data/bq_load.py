from pandas.io import gbq
import pandas_gbq
import gcsfs

def bigquery_load(df, table_name, dataset_name, project_id):
    '''
    Load table (df) into BigQuery towards target location (project/dataset/table)
    '''
    destination_table = '{}.{}'.format(dataset_name, table_name)
    df.to_gbq(destination_table, project_id, if_exists = 'replace')