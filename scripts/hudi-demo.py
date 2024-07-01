from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import lit, col

hudi_spark_jar = 'org.apache.hudi:hudi-spark3.5-bundle_2.13:0.15.0,org.apache.spark:spark-avro_2.13:3.5.1'

conf = SparkConf().setAppName("hudi-demo") \
    .set("spark.kyro.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .set("spark.jars.packages", hudi_spark_jar)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

tableName = "trips_table"
basePath = "warehouse/hudi_trips_table"

# INSERT DATA
columns = ["ts","uuid","rider","driver","fare","city"]
data =[(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-C","driver-M",27.70 ,"san_francisco"),
    (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-D","driver-L",33.90 ,"san_francisco"),
    (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo"),
    (1695115999911,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",17.85,"chennai")]
inserts = spark.createDataFrame(data).toDF(*columns)

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'city'
}

inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)

print("Checking schema of trips_table")
spark.read.format("hudi").load(basePath).printSchema()

# QUERY DATA
tripsDF = spark.read.format("hudi").load(basePath)
tripsDF.createOrReplaceTempView("trips_table")

spark.sql("SELECT uuid, fare, ts, rider, driver, city FROM  trips_table WHERE fare > 20.0").show()
spark.sql("SELECT _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare FROM trips_table").show()

# UPDATE DATA
# Lets read data from target Hudi table, modify fare column for rider-D and update it.
updatesDf = spark.read.format("hudi").load(basePath).filter("rider == 'rider-D'").withColumn("fare",col("fare")*10)

updatesDf.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(basePath)

# MERGING DATA
adjustedFareDF = spark.read.format("hudi").load(basePath). \
    limit(2).withColumn("fare", col("fare") * 100)
adjustedFareDF.write.format("hudi"). \
option("hoodie.datasource.write.payload.class","com.payloads.CustomMergeIntoConnector"). \
mode("append"). \
save(basePath)
# Notice Fare column has been updated but all other columns remain intact.
spark.read.format("hudi").load(basePath).show()

# DELETE DATA
# Lets  delete rider: rider-D
deletesDF = spark.read.format("hudi").load(basePath).filter("rider == 'rider-F'")

# issue deletes
hudi_hard_delete_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.datasource.write.operation': 'delete',
}

deletesDF.write.format("hudi"). \
    options(**hudi_hard_delete_options). \
    mode("append"). \
    save(basePath)

# INCREMENTAL QUERY
# reload data
spark.read.format("hudi").load(basePath).createOrReplaceTempView("trips_table")

commits = list(map(lambda row: row[0], spark.sql("SELECT DISTINCT(_hoodie_commit_time) AS commitTime FROM  trips_table ORDER BY commitTime").limit(50).collect()))
beginTime = commits[len(commits) - 2] # commit time we are interested in

# incrementally query data
incremental_read_options = {
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.begin.instanttime': beginTime,
}

tripsIncrementalDF = spark.read.format("hudi"). \
    options(**incremental_read_options). \
    load(basePath)
tripsIncrementalDF.createOrReplaceTempView("trips_incremental")

spark.sql("SELECT `_hoodie_commit_time`, fare, rider, driver, uuid, ts FROM trips_incremental WHERE fare > 20.0").show()

# CHANGE DATA CAPTURE
# Lets first insert data to a new table with cdc enabled.
columns = ["ts","uuid","rider","driver","fare","city"]
data =[(1695159649087,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"),
    (1695091554788,"e96c4396-3fad-413a-a942-4cb36106d721","rider-B","driver-L",27.70 ,"san_francisco"),
    (1695046462179,"9909a8b1-2d15-4d3d-8ec9-efc48c536a00","rider-C","driver-M",33.90 ,"san_francisco"),
    (1695516137016,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-C","driver-N",34.15,"sao_paulo")]

inserts = spark.createDataFrame(data).toDF(*columns)

hudi_options = {
    'hoodie.table.name': tableName,
    'hoodie.datasource.write.partitionpath.field': 'city',
    'hoodie.table.cdc.enabled': 'true'
}
# Insert data
inserts.write.format("hudi"). \
    options(**hudi_options). \
    mode("overwrite"). \
    save(basePath)


#  Update fare for riders: rider-A and rider-B 
updatesDf = spark.read.format("hudi").load(basePath).filter("rider == 'rider-A' or rider == 'rider-B'").withColumn("fare",col("fare")*10)

updatesDf.write.format("hudi"). \
    mode("append"). \
    save(basePath)

# Query CDC data
cdc_read_options = {
    'hoodie.datasource.query.incremental.format': 'cdc',
    'hoodie.datasource.query.type': 'incremental',
    'hoodie.datasource.read.begin.instanttime': 0
}
spark.read.format("hudi"). \
    options(**cdc_read_options). \
    load(basePath).show(10, False)
