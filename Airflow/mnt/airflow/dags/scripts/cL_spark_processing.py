from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession

# Define the warehouse location for Spark
warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Craigslist Listings Processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the CSV file from HDFS
df = spark.read.option("header", "true").csv('hdfs://namenode:9000/craigslist_listings/ready_for_database.csv')


# Export the dataframe into the Hive table craigslist_listings
df.write.mode("append").insertInto("craigslist_listings")
