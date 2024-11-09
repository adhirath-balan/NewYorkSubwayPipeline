from pyspark.sql import SparkSession, functions as f

spark = SparkSession.builder\
        .master("yarn")\
        .appName('DataManagement3')\
        .getOrCreate()

df = spark.read.csv(
    "gs://data_management_3/*.csv",
    sep = ",",
    header = True
)

df = df.drop("departure_time")
df.write.csv("gs://data_management_3/final_data.csv")