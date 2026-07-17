from pyspark.sql import SparkSession

print("Starting cluster... this takes a few seconds...")
spark = (
    SparkSession.builder
    .appName("cluster_check")
    .master("spark://master:7077")
    .getOrCreate()
)

print("CLUSTER IS ALIVE!")
print(spark.sparkContext)
print("Workers:", spark.sparkContext.defaultParallelism)

spark.stop()
