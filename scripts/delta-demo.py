from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta import *
from delta.tables import *
from pyspark.sql.functions import expr, lit, col

jars = 'io.delta:delta-spark_2.12:3.2.0,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:3.2.0'

# Setup config
conf = SparkConf().setAppName("deltaDemo") \
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .set('spark.jars.packages', jars)

# Create spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create a spark dataframe and write as a delta table
print("Starting Delta table creation")

data = [("Robert", "Baratheon", "Baratheon", "Storms End", 48),
        ("Eddard", "Stark", "Stark", "Winterfell", 46),
        ("Jamie", "Lannister", "Lannister", "Casterly Rock", 29)
        ]
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("house", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(data=data, schema=schema)
df.write.mode(saveMode="overwrite").format("delta").save("warehouse/delta-table")

# read from delta
df = spark.read.format("delta").load("warehouse/delta-table")
df.show()

# print delta table schema
df.printSchema()

# Update data
print("Updating Delta table...!")
data = [("Robert", "Baratheon", "Baratheon", "Storms End", 49),
        ("Eddard", "Stark", "Stark", "Winterfell", 47),
        ("Jamie", "Lannister", "Lannister", "Casterly Rock", 30)
        ]
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("house", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame(data=data, schema=schema)
df.write.mode(saveMode="overwrite").format("delta").save("warehouse/delta-table")

print("Showing the data after being updated")
df.show()

# Update data in Delta
print("Conditional Update...!")

# delta table path
deltaTable = DeltaTable.forPath(spark, "warehouse/delta-table")
deltaTable.toDF().show()

deltaTable.update(
    condition=expr("firstname == 'Jamie'"),
    set={"firstname": lit("Jamie"), "lastname": lit("Lannister"), "house": lit("Lannister"),
        "location": lit("Kings Landing"), "age": lit(37)})

deltaTable.toDF().show()

# Upsert Data
print("Upserting Data...!")
# delta table path
deltaTable = DeltaTable.forPath(spark, "warehouse/delta-table")
deltaTable.toDF().show()

# define new data
data = [("Gendry", "Baratheon", "Baratheon", "Kings Landing", 19),
        ("Jon", "Snow", "Stark", "Winterfell", 21),
        ("Jamie", "Lannister", "Lannister", "Casterly Rock", 36)
        ]
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("house", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True)
])

newData = spark.createDataFrame(data=data, schema=schema)

deltaTable.alias("oldData") \
    .merge(
    newData.alias("newData"),
    "oldData.firstname = newData.firstname") \
    .whenMatchedUpdate(
    set={"firstname": col("newData.firstname"), "lastname": col("newData.lastname"), "house": col("newData.house"),
        "location": col("newData.location"), "age": col("newData.age")}) \
    .whenNotMatchedInsert(
    values={"firstname": col("newData.firstname"), "lastname": col("newData.lastname"), "house": col("newData.house"),
            "location": col("newData.location"), "age": col("newData.age")}) \
    .execute()

deltaTable.toDF().show()

# Delete Data
print("Deleting data...!")

# delta table path
deltaTable = DeltaTable.forPath(spark, "warehouse/delta-table")
deltaTable.toDF().show()

deltaTable.delete(condition=expr("firstname == 'Gendry'"))

deltaTable.toDF().show()

# Reading Older version of Data
print("Read old data...!")

print("Dataframe Version 0")
df_ver0 = spark.read.format("delta").option("versionAsOf", 0).load("warehouse/delta-table")
df_ver0.show()

print("Dataframe Version 1")
df_ver1 = spark.read.format("delta").option("versionAsOf", 1).load("warehouse/delta-table")
df_ver1.show()
