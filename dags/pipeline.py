from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from google.api_core.retry import Retry
from datetime import datetime, timedelta
from train_fetcher import get_train_data, bucket_client  # Import your function from the appropriate module
from dataproc import PROJECT_ID, REGION, CLUSTER_NAME

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 8),
    'retry_delay': timedelta(minutes=5),
}


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
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
        op_kwargs={'NUMBER_OF_FETCHES': 5, 'WAIT_SECONDS': 10, 'client' : bucket_client},
        dag=dag,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )

    create_cluster >> fetch_train_data_task


