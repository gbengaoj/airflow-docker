from datetime import timedelta # Required to calculate duration
import airflow # Required to instantiate a DAG
import pandas as pd
import numpy as np
import os 

from airflow import DAG
from airflow.operators.python import PythonOperator # Python opeator will be used in our task to run any python code


dag_path = os.getcwd()


def data_cleaning():
    hotel_data = pd.read_csv("raw_data/hotel_bookings.csv") # Importing our raw data.
    hotel_data.head() # Displaying the first rows of our dataframe
    #hotel_data.info() # Printing information about our dataframe
    hotel_data.describe() # Returning the description of data in our data frame

    # Checking null columns
    hotel_data.isnull().sum()

    # Replacing missing values
    nan_replacements = {'children':0, 'country': 'Unknown', 'agent': 'Organic Booking',
    'company': 'Personal Booking'}
    cleaned_data = hotel_data.fillna(nan_replacements)

    #cleaned_data.info()

    # Storing cleaned data.
    cleaned_data.to_csv("processed_data/processed_hotel_data.csv", index=False)


def cleaned_data_message():
    print("Data successfully cleaned")


# These arguments will be passed to each operator.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7), # The date which the dag should start running
}

# Instiate pipeline dynamically
data_cleaning_dag = DAG(
    'data_cleaning_dag', # name of the gag displayed in airflow user interface (UI)
    default_args=default_args, #
    schedule_interval=timedelta(days=30), # The interval at which the dag should run
    catchup=False
)

# Tasks
clean_data = PythonOperator(
    task_id='data_cleaning',
    python_callable=data_cleaning,
    dag=data_cleaning_dag,
)

message = PythonOperator(
    task_id='cleaned_data_message',
    python_callable=cleaned_data_message,
    dag=data_cleaning_dag
)

# Task dependency
clean_data >> message # We'll need to run the clean_data task first, followed by the message