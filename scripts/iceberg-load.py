from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

warehouse_path = "./warehouse"
iceberg_spark_jar = 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3'
iceberg_spark_ext = 'org.apache.iceberg:iceberg-spark-extensions-3.4_2.12:1.4.3'
catalog_name = "demo"

# Setup iceberg config
conf = SparkConf().setAppName("YourAppName") \
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .set('spark.jars.packages', iceberg_spark_jar) \
    .set('spark.jars.packages', iceberg_spark_ext) \
    .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
    .set(f"spark.sql.catalog.{catalog_name}.type", "hadoop")\
    .set("spark.sql.defaultCatalog", catalog_name)

# Create spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SOURCE_PATH = "./data/yellow_trip_data"
DEST_PATH = "./data/output/count_by_vendor.parquet"

# Loading the data
df = spark.read.options(inferSchema=True).parquet(SOURCE_PATH)
df.printSchema()

spark.sql(f"CREATE DATABASE IF NOT EXISTS db")
df.writeTo("db.yellow_trip_data") \
    .createOrReplace()

spark.sql("DESCRIBE TABLE db.yellow_trip_data").show(truncate=False)
spark.sql("SHOW CREATE TABLE db.yellow_trip_data").show(truncate=False)
spark.sql("SELECT * FROM db.yellow_trip_data LIMIT 5").show()
