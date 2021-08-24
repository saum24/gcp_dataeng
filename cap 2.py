#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Importing all the necessary packages and operators
import os
import pandas as pd
import dask.dataframe as dd
import json
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'Cap2_DS')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'Mod2_Proj2')

dag = models.DAG(
    dag_id='gcs_to_bigquery_operator',
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['example']
)

#Converting string column into dict column
def string_to_dict(dict_string):
    # Convert to proper json format
    dict_string = dict_string.replace("'", '"')
    return json.loads(dict_string)

# Function to concatanate all the csv files and perform necessary transformations
def transform_data():
    #Read .CSV files into dask dataframe
    dask_df = dd.read_csv('gs://saums_bucket/egen_cap1/*.csv') 
    #Convert the dask df into pandas df
    pandas_df = dask_df.compute()
    pandas_df['dict_col']=pandas_df['1d'].apply(string_to_dict)
    
    #Flattening the dict column and merging it with original dataframe
    flattened_df = pd.json_normalize(pandas_df['dict_col'])
    mergedDf = pandas_df.merge(flattened_df, left_index=True, right_index=True)
    transformed_df = mergedDf[['name','price','price_timestamp','market_cap','volume','price_change', 'price_change_pct', 'volume_change',
                               'volume_change_pct', 'market_cap_change', 'market_cap_change_pct']]
    transformed_df['price_timestamp'] = pd.to_datetime(transformed_df['price_timestamp'])
    transformed_df = transformed_df[['name', 'price','price_change','price_change_pct','price_timestamp',
                                     'market_cap','market_cap_change', 'market_cap_change_pct','volume','volume_change', 'volume_change_pct']]

    transformed_df['price_timestamp'] = transformed_df['price_timestamp'].dt.strftime("%m-%d-%Y %H:%M:%S")
    transformed_df['price_timestamp'] = pd.to_datetime(transformed_df['price_timestamp'])

    
    transformed_df.to_csv('gs://saums_bucket/egen_cap2/transformed.csv', index=False)
    
# Calling the transform function into a Python operator
transform_csv = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_data,
    dag=dag
    )

# Create a dataset in BigQuery

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bq_dataset', dataset_id=DATASET_NAME, dag=dag
)

# Loading the transformed csv file into BQ Table

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='saums_bucket',
    source_objects=['egen_cap2/transformed.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    
    schema_fields=[

        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'price_change', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'price_change_pct', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'price_timestamp', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'market_cap', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'market_cap_change', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'market_cap_change_pct', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'volume', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'volume_change', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'volume_change_pct', 'type': 'FLOAT', 'mode': 'NULLABLE'}],

    write_disposition='WRITE_APPEND',
    skip_leading_rows = 1,
    dag=dag
)


delete_dataset = BigQueryDeleteDatasetOperator(task_id='delete_bq_dataset', dataset_id=DATASET_NAME, delete_contents=True, dag=dag)

# Setting up the task sequence for the DAG
transform_csv >> create_dataset >> load_csv

