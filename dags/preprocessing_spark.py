from pyspark.sql import SparkSession, functions as f
from google.cloud import bigquery

# Configuration parameters
LOCAL_USER = "leon"
# LOCAL_USER = "adhirath"

if LOCAL_USER == "leon":
    BUCKET_NAME = "data_management_3"
    PROJECT_ID = "data-management-3-440208"
    DATASET_ID =  "new_project"
    TABLE_ID = "new_table"
    APP_NAME = 'DataManagement3'
    GS_CSV_PATH = "gs://data_management_3/*.csv"
elif LOCAL_USER == "adhirath":
    BUCKET_NAME = "data_management_2"
    PROJECT_ID = "adb-dm2"
    DATASET_ID =  "new_project"
    TABLE_ID = "new_table"
    APP_NAME = 'DataManagement2'
    GS_CSV_PATH = "gs://data_management_2/*.csv"
    


def preprocessing_big_data(bucket_name, project_id, dataset_id, table_id):
    spark = SparkSession.builder\
            .master("yarn")\
            .appName(APP_NAME)\
            .getOrCreate()
    
    spark.conf.set("temporaryGcsBucket", bucket_name)

    df = spark.read.csv(
        GS_CSV_PATH,
        sep = ",",
        header = True
    )

    df = df.withColumn("departure_time", f.to_timestamp("departure_time", "yyyy-MM-dd HH:mm:ss.SSS"))

    df = df.withColumn("current_location", f.regexp_extract("readable", r'\b(?:INCOMING_AT|STOPPED_AT|IN_TRANSIT_TO)\s+([^,]*)', 1))

    df = df.withColumn("current_location",
                        f.when(f.col("current_location") == "", "Unknown").otherwise(f.col("current_location")))
    
    df = df.na.fill({"location" : 0, 
                     "headsign_text" : "Unknown", 
                     "last_position_update" : "No_Update"})
    
    df = df.withColumn("location_status", f.when(f.col("readable").contains("Currently"), f.col("location_status"))
                   .otherwise(f.when(f.col("readable").contains("- train assigned"), "TRAIN_ASSIGNED")
                              .otherwise("NO_TRAIN_ASSIGNED")))
    
    create_project_and_dataset(project_id, dataset_id, table_id)
    print(df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df.columns]).show())
    print(df.printSchema())
    print(df.show())
    df.write.format("bigquery").option("table", f"{dataset_id}.{table_id}").mode("overwrite").save()

def create_project_and_dataset(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)

    # Create dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)  # Check if dataset exists
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set location as needed
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    # Create table if it doesn't exist
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        # Define the schema
        schema = [
            bigquery.SchemaField("readable", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("direction", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("route_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("headsign_text", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("departure_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("location_status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("last_position_update", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("current_location", "STRING", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created.")

    return table_ref

if __name__ == "__main__":
    preprocessing_big_data(BUCKET_NAME, PROJECT_ID, DATASET_ID, TABLE_ID)