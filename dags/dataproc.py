from google.cloud import dataproc_v1 as dataproc
from google.oauth2 import service_account

# Configuration parameters
PROJECT_ID = "data-management-3-440208"  # Replace with your Google Cloud project ID omega-keep-411319
CLUSTER_NAME = "data-management-3"
REGION = "us-central1"  # Replace with your cluster's region
CREDENTIAL_PATH = "./opt/airflow/creds/cred.json"

def run_dataproc_job(CLUSTER_NAME, PROJECT_ID, REGION):
    """Submits and Runs PySpark job to the Dataproc cluster."""

    # Initialize clients
    credentials = service_account.Credentials.from_service_account_file(CREDENTIAL_PATH)
    dataproc_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"},
        credentials=credentials,
    )

    # Initialize request argument(s)
    job = dataproc.Job()
    job.pyspark_job.main_python_file_uri = "gs://data_management_3/preprocessing_spark.py"
    job.placement.cluster_name = CLUSTER_NAME

    request = dataproc.SubmitJobRequest(
        project_id=PROJECT_ID,
        region=REGION,
        job=job,
    )

    # Make the request
    operation = dataproc_client.submit_job_as_operation(request=request)

    print("Waiting for operation to complete...")

    response = operation.result()

    # Handle the response
    print("Job finished : ",response)

if __name__ == "__main__":
    run_dataproc_job(CLUSTER_NAME, PROJECT_ID, REGION)