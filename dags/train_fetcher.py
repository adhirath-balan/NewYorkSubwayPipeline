from nyct_gtfs import NYCTFeed
import pandas as pd
from google.cloud import storage
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Configuration parameters
# LOCAL_USER = "leon"
LOCAL_USER = "adhirath"

NUMBER_OF_FETCHES = 5
WAIT_SECONDS = 10
if LOCAL_USER == "leon":
    BUCKET_NAME = "data_management_3"
elif LOCAL_USER == "adhirath":
    BUCKET_NAME = "data_management_2"

bucket_client = storage.Client()

def get_train_data(NUMBER_OF_FETCHES, WAIT_SECONDS, client):
    endpoints = ["https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
             "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si"]

    columns = ["readable","direction", "route_id", "headsign_text", "departure_time", "location", "location_status", "last_position_update"]

    for i in range(NUMBER_OF_FETCHES):
        logger.info(f"Starting run: {i}")

        list_rows = []
        for endpoint in endpoints:
            feed = NYCTFeed(endpoint)
            all_trains = feed.trips
            

            for t in all_trains:
                try:
                    readable = str(t)
                    direction = t.direction
                    route_id = t.route_id
                    headsign_text = t.headsign_text
                    departure_time = t.departure_time
                    location = t.location
                    location_status = t.location_status
                    last_position_update = t.last_position_update

                    row =  [readable,direction, route_id, headsign_text, departure_time, location, location_status, last_position_update]
                    list_rows.append(row)
                except Exception as e:
                    logger.error(e)

        df_train_data = pd.DataFrame(list_rows, columns=columns) 

        ts = str(time.time())
        ts = ts.replace(".", "_")

        data_to_gcp_bucket(df_train_data, client, f"train_data_{ts}.csv")

        logger.info(f"Completed run: {i}")
        logger.info(f"Waiting before next run")
        time.sleep(WAIT_SECONDS)


def data_to_gcp_bucket(df, client, csv_name):
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = client.create_bucket(BUCKET_NAME)
        logging.info(f"Created bucket {BUCKET_NAME}")

    blob = bucket.blob(csv_name)
    csv_data = df.to_csv(index=False)

    blob.upload_from_string(csv_data, content_type='text/csv')

if __name__ == "__main__":
    get_train_data(2, 5, bucket_client)