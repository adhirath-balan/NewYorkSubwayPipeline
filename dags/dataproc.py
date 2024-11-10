from google.cloud import dataproc_v1 as dataproc

# Configuration parameters
PROJECT_ID = "adb-dm2"  # Replace with your Google Cloud project ID omega-keep-411319
CLUSTER_NAME = "data-management-2"
REGION = "us-central1"  # Replace with your cluster's region

def run_dataproc_job(CLUSTER_NAME, PROJECT_ID, REGION):
    """Submits and Runs PySpark job to the Dataproc cluster."""

    # Initialize clients
    dataproc_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"},
    )

    # Initialize request argument(s)
    job = dataproc.Job()
    job.pyspark_job.main_python_file_uri = "gs://data_management_2/preprocessing_spark.py"
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