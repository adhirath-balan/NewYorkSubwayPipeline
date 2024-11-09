from google.cloud import dataproc_v1 as dataproc
from google.oauth2 import service_account
import os

# Configuration parameters
PROJECT_ID = "adb-dm2"  # Replace with your Google Cloud project ID omega-keep-411319
CLUSTER_NAME = "data-management-2"
REGION = "us-central1"  # Replace with your cluster's region
PYTHON_FILE_NAME = "./dags/preprocessing_spark.py"  # Local path to your Python script

def submit_job(dataproc_client):
    """Submits a PySpark job to the Dataproc cluster."""
    # Ensure the Python file is accessible
    # if not os.path.isfile(PYTHON_FILE_NAME):
    #     raise FileNotFoundError(f"The specified file {PYTHON_FILE_NAME} does not exist.")

    print(os.path.abspath(PYTHON_FILE_NAME))
    # job = {
    #     "placement": {"cluster_name": CLUSTER_NAME},
    #     "pyspark_job": {
    #         "main_python_file_uri" : "gs://data_management_2/preprocessing_spark.py",
    #     }
    # }

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
    print(response)

    # operation = dataproc_client.submit_job_as_operation(
    #     request={"project_id": PROJECT_ID, "region": REGION, "job": job}
    # )
    # response = operation.result()
    # print("Job finished:", response)
    
def create_cluster():
    # Create the cluster client.
    print(PROJECT_ID, CLUSTER_NAME, REGION)
    credentials = service_account.Credentials.from_service_account_file("D:\\SRH\\DM-2\\Project\\creds\\cred.json")
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"},
        credentials = credentials,
    )

    # Create the cluster config.
    cluster = {
        "project_id": PROJECT_ID,
        "cluster_name": CLUSTER_NAME,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 500},
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
                "disk_config": {"boot_disk_size_gb": 500},
            },
            "software_config": {
                "image_version": "2.2-debian12",  # Specify your Dataproc image version
            },
            "gce_cluster_config": {
                "subnetwork_uri": f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default",  # Specify your subnetwork URI
            },
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": PROJECT_ID, "region": REGION, "cluster": cluster},
        timeout=1200,
    )
    result = operation.result()

    print("Cluster created successfully: {}".format(result.cluster_name))

def main():
    # create_cluster()
    # Initialize clients
    credentials = service_account.Credentials.from_service_account_file("D:\\SRH\\DM-2\\Project\\creds\\cred.json")
    dataproc_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"},
        credentials=credentials,
    )

    # # dataproc_client = dataproc.JobControllerClient()

    # # Step 3: Submit job to Dataproc
    submit_job(dataproc_client)

if __name__ == "__main__":
    main()