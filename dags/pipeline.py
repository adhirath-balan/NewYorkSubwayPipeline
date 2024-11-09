from airflow import DAG
from airflow.operators.python import PythonOperator
from google.api_core.retry import Retry
from datetime import datetime, timedelta
from train_fetcher import get_train_data, bucket_client  # Import your function from the appropriate module
from dataproc import PROJECT_ID, REGION, CLUSTER_NAME, run_dataproc_job

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 8),
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'train_data_fetcher',
    default_args=default_args,
    description='A simple DAG to fetch train data from MTA API',
    schedule_interval='0 0 1 */6 *',  # Adjust as needed
) as dag :

    # Define the task
    fetch_train_data_task = PythonOperator(
        task_id='fetch_train_data',
        python_callable=get_train_data,
        op_kwargs={
            'NUMBER_OF_FETCHES': 5, 
            'WAIT_SECONDS': 10, 
            'client' : bucket_client
        },
    )

    run_job = PythonOperator(
        task_id="run_job",
        python_callable=run_dataproc_job,
        op_kwargs={
            "PROJECT_ID" : PROJECT_ID,
            "REGION" : REGION,
            "CLUSTER_NAME" : CLUSTER_NAME
        }
    )

    run_job >> fetch_train_data_task