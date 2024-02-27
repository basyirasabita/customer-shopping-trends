'''
=================================================
Milestone 3

Name  : Basyira Sabita
Batch : FTDS-012-HCK

This program aims to automate load data from PostgreSQL, data cleaning, and upload data to ElasticSearch. 
The data used in this program is the Customer Shopping Trends Dataset.
=================================================
'''

from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

# Connect to Postgres
from sqlalchemy import create_engine

def fetch_data_from_postgres(database, username, password):
    '''
        This function is used to fetch the data from local postgres database

        Params:
            - database (str): the name of the local database
            - username (str): the name of username used to access the local database
            - password (str): the password used to access the local database

        Return: -

        Example of use: fetch_data_from_postgres('database', 'username', 'password')
    '''
    host = "postgres"

    # Declare URL to connect to Postgres
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Use URL to connect to sqlalchemy
    engine = create_engine(postgres_url)
    connection = engine.connect()

    # Fetch data using sql query
    df = pd.read_sql_query("select * from table_m3", connection) 
    df.to_csv('/opt/airflow/dags/P2M3_basyira_sabita_data_raw.csv', sep=',', index=False)

def data_cleaning():
    '''
        This function is used to pre-processing the fetched data by cleaning it

        Params: -

        Return: -
        
        Example of use: data_cleaning()
    '''
    df_raw = pd.read_csv('/opt/airflow/dags/P2M3_basyira_sabita_data_raw.csv')
    df = df_raw.copy()

    columns = df.columns
    new_col_names = {}

    for col in columns:
        col_remove_char = col.replace('(', '').replace(')', '')

        col_split = col_remove_char.split(' ')
        for i in range(len(col_split)):
            word_lower = col_split[i].lower()
            col_split[i] = word_lower
        new_col_name = '_'.join(col_split)

        new_col_names[col] = new_col_name

    df = df.rename(columns = new_col_names)

    # Dropping Missing Values
    df.dropna(inplace=True)

    # Dropping Duplicates
    df.drop_duplicates(inplace=True)

    # Saving to new csv
    df.to_csv('/opt/airflow/dags/P2M3_basyira_sabita_data_clean.csv', index=False)


def upload_to_elasticsearch():
    '''
        This function is used to upload the cleaned data to elasticsearch for visualization

        Params: -

        Return: -
        
        Example of use: upload_to_elasticsearch()
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_basyira_sabita_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
    

default_args = {
    'owner': 'Basyira', 
    'start_date': datetime(2023, 2, 22, 12, 00) - timedelta(hours=7)
}

with DAG(
    "P2M3_basyira_sabita_DAG",
    description='Milestone 3',
    schedule_interval='30 6 * * *', # Triggered at 6:30 AM
    default_args=default_args, 
    catchup=False
) as dag:
    # Task 1 - Fetch data from postgres
    fetch_data_task = PythonOperator(
        task_id = 'fetch_data_from_postgres',
        python_callable = fetch_data_from_postgres,
        op_kwargs = {
            'database': 'airflow_m3',
            'username': 'airflow_m3',
            'password': 'airflow_m3'
        }
    )

    # Task 2 - Data cleaning
    data_cleaning_task = PythonOperator(
        task_id = 'data_cleaning',
        python_callable = data_cleaning
    )

    # Task 3 - Upload data to elasticsearch
    upload_data_task = PythonOperator(
        task_id = 'upload_data_to_elasticsearch',
        python_callable = upload_to_elasticsearch
    )

    # Tasks pipeline
    fetch_data_task >> data_cleaning_task >> upload_data_task