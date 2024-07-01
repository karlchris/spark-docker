from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

jars = 'io.delta:delta-spark_2.12:3.2.0,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:3.2.0'

# Setup iceberg config
conf = SparkConf().setAppName("deltaDemo") \
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .set('spark.jars.packages', jars)

# Create spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a dataframe
data = [("John", "Doe", 23), ("Jane", "Doe", 22), ("Tom", "Smith", 25)]
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("age", IntegerType(), True)])
df = spark.createDataFrame(data, schema=schema)

# write to delta
df.write.format("delta").mode("overwrite").save("warehouse/delta-table")

# read from delta
df = spark.read.format("delta").load("warehouse/delta-table")
df.show()

# print delta table schema
df.printSchema()
