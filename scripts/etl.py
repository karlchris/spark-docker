from pyspark.sql import SparkSession

# Initialize spark session
spark = (
    SparkSession.
    builder.
    appName("etl_demo").
    config("spark.cores.max", "4").
    config("spark.sql.shuffle.partitions", "5").
    getOrCreate()
)

SOURCE_PATH = "./data/yellow_trip_data"
DEST_PATH = "./data/output/count_by_vendor.parquet"

# Loading the data
df = spark.read.options(inferSchema=True).parquet(SOURCE_PATH)
df.createOrReplaceTempView("yellow_trip_data")
df.show(5)

# Transformation: Count
df2 = df.groupBy("VendorID").count()
df2.write.mode("overwrite").parquet(DEST_PATH)

# Viewing Output
df3 = spark.read.options(inferSchema=True).parquet(DEST_PATH)
df3.createOrReplaceTempView("count_by_vendor")
spark.sql("select * from count_by_vendor").show()
