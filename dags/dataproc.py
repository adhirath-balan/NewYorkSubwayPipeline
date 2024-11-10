from google.cloud import dataproc_v1 as dataproc
from google.cloud import exceptions
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Configuration parameters
# LOCAL_USER = "leon"
LOCAL_USER = "adhirath"

if LOCAL_USER == "leon":
    PROJECT_ID = "data-management-3-440208"
    CLUSTER_NAME = "data-management-3"
    REGION = "us-central1"
    SPARK_FILE = "gs://data_management_3/preprocessing_spark.py"
elif LOCAL_USER == "adhirath":
    PROJECT_ID = "adb-dm2"
    CLUSTER_NAME = "data-management-2"
    REGION = "us-central1"
    SPARK_FILE = "gs://data_management_2/preprocessing_spark.py"


def run_dataproc_job(CLUSTER_NAME, PROJECT_ID, REGION):
    """Submits and Runs PySpark job to the Dataproc cluster."""

    start_cluster(PROJECT_ID, REGION, CLUSTER_NAME)
    dataproc_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"},
    )

    job = dataproc.Job()
    job.pyspark_job.main_python_file_uri = SPARK_FILE
    job.placement.cluster_name = CLUSTER_NAME

    request = dataproc.SubmitJobRequest(
        project_id=PROJECT_ID,
        region=REGION,
        job=job,
    )

    operation = dataproc_client.submit_job_as_operation(request=request)

    logger.info("Waiting for operation to complete...")

    response = operation.result()

    logger.info("Job finished : ",response)
    stop_cluster(PROJECT_ID, REGION, CLUSTER_NAME)

def stop_cluster(project_id, region, cluster_name):
    """Stops a Dataproc cluster."""
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"})
    try:
        request = dataproc.StopClusterRequest(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
        )

        operation = cluster_client.stop_cluster(request=request)
        logger.info(f'Stopping cluster {cluster_name}...')
        operation.result() 
        logger.info(f'Cluster {cluster_name} stopped.')
    except exceptions.NotFound:
        logger.info(f'Cluster {cluster_name} not found.')
    except Exception as e:
        logger.error(f'Error stopping cluster: {e}')

def start_cluster(project_id, region, cluster_name):
    """Starts a Dataproc cluster if it is not already running."""
    status = get_cluster_status(project_id, region, cluster_name)
    
    if status == dataproc.ClusterStatus.State.RUNNING:
        logger.info(f'Cluster {cluster_name} is already running.')
    elif status in [dataproc.ClusterStatus.State.STOPPED, dataproc.ClusterStatus.State.STOPPING]:
        logger.info(f'Starting cluster {cluster_name}...')
        client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"})

        request = dataproc.StartClusterRequest(
            project_id=project_id,
            region=region,
            cluster_name=cluster_name,
        )

        operation = client.start_cluster(request=request)
        
        operation.result()
        logger.info(f'Cluster {cluster_name} started.')
    else:
        logger.error(f'Cluster {cluster_name} is in state: {status}. Cannot start.')

def get_cluster_status(project_id, region, cluster_name):
    """Gets the status of a Dataproc cluster."""
    cluster_client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"})
    try:
        cluster = cluster_client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
        return cluster.status.state
    except exceptions.NotFound:
        logger.error(f'Cluster {cluster_name} not found.')
        return None
    except Exception as e:
        logger.error(f'Error retrieving cluster status: {e}')
        return None

if __name__ == "__main__":
    run_dataproc_job(CLUSTER_NAME, PROJECT_ID, REGION)