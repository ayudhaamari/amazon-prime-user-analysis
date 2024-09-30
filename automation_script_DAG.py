'''
=================================================
Author  : Ayudha Amari Hirtranusi

This program is designed to automate the transformation and loading of data from PostgreSQL to ElasticSearch using Apache Airflow.
The dataset used is the Amazon Prime userbase dataset from Kaggle.
=================================================
'''

# Importing Libraries

# Date and time libraries
import datetime as dt
from datetime import timedelta

# Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator

# Data processing and database libraries
from elasticsearch import Elasticsearch
import pandas as pd
import psycopg2 as db

def fetch_from_postgresql():
    """
    Fetches data from PostgreSQL and saves it as a CSV file.

    This function establishes a connection to a PostgreSQL database using predefined credentials,
    executes a SQL query to select all data from the 'amazon_prime_userbase' table,
    and saves the results to a CSV file named 'P2M3_Ayudha_Amari_data_raw.csv' in the '/opt/airflow/dags/' directory.

    Parameters:
    None

    Returns:
    None

    Side effects:
    - Creates a new CSV file named 'P2M3_Ayudha_Amari_data_raw.csv' in the '/opt/airflow/dags/' directory.
    - Prints "-------Data Saved------" to the console upon successful completion.

    Example usage:
    fetch_from_postgresql()
    """
    # Define the connection string for PostgreSQL
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    
    # Establish a connection to the PostgreSQL database
    conn = db.connect(conn_string)
    
    # Execute SQL query to fetch all data from amazon_prime_userbase table
    df = pd.read_sql("select * from amazon_prime_userbase", conn)
    
    # Save the fetched data to a CSV file
    df.to_csv('/opt/airflow/dags/P2M3_Ayudha_Amari_data_raw.csv', index=False)
    
    # Print a message indicating successful data save
    print("-------Data Saved------")

def data_cleaning():
    """
    Performs data cleaning on the raw CSV file.

    This function reads the raw CSV file, performs various cleaning operations,
    and saves the cleaned data to a new CSV file.

    Operations include:
    - Removing duplicate data
    - Normalizing column names (lowercase, replace spaces with underscores)
    - Removing unnecessary spaces/tabs/symbols from column names
    - Handling missing values

    Parameters:
    None

    Returns:
    None

    Side effects:
    - Creates a new CSV file named 'P2M3_Ayudha_Amari_data_clean.csv' in the '/opt/airflow/dags/' directory.

    Example usage:
    data_cleaning()
    """
    # Read the raw CSV file
    df = pd.read_csv('/opt/airflow/dags/P2M3_Ayudha_Amari_data_raw.csv')
    
    # Remove duplicate data
    df.drop_duplicates(inplace=True)
    
    # Normalize column names
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'[^\w\s]', '', regex=True)
    
    # Handle missing values
    # We'll drop rows with any missing values because this data actually doesn't have any missing values, and the if there are any, the data with missing values in this kind of dataset usually will be <5% (very few) so dropping is the best option here
    # If we want to take another approach, filling the categorical with mode and numerical with mean also can be considered for certain problem/dataset, but for this case, we won't do it
    df.dropna(inplace=True)
    
    # Save the cleaned data
    df.to_csv('/opt/airflow/dags/P2M3_Ayudha_Amari_data_clean.csv', index=False)
    print("Data cleaning completed. Cleaned data saved to 'P2M3_Ayudha_Amari_data_clean.csv'")

def insert_to_elasticsearch():
    """
    Inserts cleaned data from a CSV file into Elasticsearch.

    This function connects to an Elasticsearch instance, reads data from the cleaned CSV file,
    and inserts each row of the CSV as a separate document into the Elasticsearch index 'amazon_prime_userbase'.

    Parameters:
    None

    Returns:
    None

    Side effects:
    - Inserts multiple documents into the Elasticsearch index 'amazon_prime_userbase'.
    - Prints the result of each insertion operation to the console.

    Example usage:
    insert_to_elasticsearch()
    """
    # Create an Elasticsearch client
    es = Elasticsearch('http://elasticsearch:9200')
    
    # Read the cleaned CSV file
    df = pd.read_csv('/opt/airflow/dags/P2M3_Ayudha_Amari_data_clean.csv')
    
    # Iterate through each row in the DataFrame
    for i, r in df.iterrows():
        # Convert the row to a dictionary
        doc = r.to_dict()
        # Index the document in Elasticsearch
        res = es.index(index="amazon_prime_userbase", id=i+1, body=doc)
        # Print the response from Elasticsearch
        print(f"Response from Elasticsearch: {res}")

# Define default arguments for the DAG
default_args = {
    'owner': 'Ayudha',  # The owner of this DAG
    'start_date': dt.datetime(2024, 9, 11, 0, 0, 0) - dt.timedelta(hours=7),  # Start date set to September 11, 2024, adjusted for timezone
    'retries': 1,  # Number of retries if a task fails
    'retry_delay': dt.timedelta(minutes=1),  # Delay between retries
}
# These default arguments will be applied to all tasks in the DAG unless overridden
# The 'owner' field helps in identifying who is responsible for this DAG
# 'start_date' determines when the DAG should start running. It's set in the future, which is common for testing
# 'retries' and 'retry_delay' help in handling task failures by automatically retrying failed tasks

# Define the DAG
with DAG('Amazon_Prime_Userbase_Processing',
         default_args=default_args,
         schedule_interval='30 6 * * *',  # Run daily at 6:30 AM
         catchup=False
         ) as dag:

    # Define tasks
    
    # Task 1: Fetch data from PostgreSQL
    # This task uses the fetch_from_postgresql function to extract data from the PostgreSQL database
    fetch_data = PythonOperator(
        task_id='fetch_data_postgresql',
        python_callable=fetch_from_postgresql
    )
    
    # Task 2: Clean the fetched data
    # This task uses the data_cleaning function to process and clean the data extracted from PostgreSQL
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=data_cleaning
    )
    
    # Task 3: Insert cleaned data into Elasticsearch
    # This task uses the insert_to_elasticsearch function to load the cleaned data into Elasticsearch
    insert_data = PythonOperator(
        task_id='insert_data_elasticsearch',
        python_callable=insert_to_elasticsearch
    )

# Define task dependencies
fetch_data >> clean_data >> insert_data