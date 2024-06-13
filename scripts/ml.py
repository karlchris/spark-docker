from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Initialize spark session
spark = (
    SparkSession.
    builder.
    appName("ml_demo").
    config("spark.cores.max", "4").
    config("spark.sql.shuffle.partitions", "5").
    getOrCreate()
)

# Loading the data
df = spark.read.options(header=True, inferSchema=True).csv('./data/retail-data/by-day')
df.createOrReplaceTempView("retail_data")
df.show(5)

# Preprocessing
pdf = df.na.fill(0).withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE")).coalesce(5)

# Split the data
train_df = pdf.where("InvoiceDate < '2011-07-01'")
test_df = pdf.where("InvoiceDate >= '2011-07-01'")

# ML Pipeline
indexer = StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
encoder = OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")
vectorAssembler = VectorAssembler().setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"]).setOutputCol("features")

tf_pipeline = Pipeline().setStages([indexer, encoder, vectorAssembler])

fitted_pipeline = tf_pipeline.fit(train_df)

# Transform Data
transformed_train = fitted_pipeline.transform(train_df)
transformed_test = fitted_pipeline.transform(test_df)

# building Model
kmeans = KMeans().setK(20).setSeed(1)
kmModel = kmeans.fit(transformed_train)

print(kmModel.summary.trainingCost)

# Predictions
predictions = kmModel.transform(transformed_test)
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

centers = kmModel.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
