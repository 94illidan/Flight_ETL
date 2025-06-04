from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TestReadCSV").getOrCreate()

df = spark.read.csv("/opt/spark/archivos/flights.csv", header=True, inferSchema=True)

df.show(5)

spark.stop()
