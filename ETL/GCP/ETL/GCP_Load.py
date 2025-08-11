# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType

# %%
def write_DB_bigquery(parquet_DF, dataset, table):
    try:
        full_table_id = f"{dataset}.{table}"
        
        parquet_DF.write \
            .format("bigquery") \
            .option("table", full_table_id) \
            .mode("overwrite") \
            .save()
    except Exception as error:
        logger.error("Error al escribir en BigQuery: " + str(error))


def read_DB_bigquery(dataset, table):
    try:
        full_table_id = f"{dataset}.{table}"
        df = spark.read \
            .format("bigquery") \
            .option("table", full_table_id) \
            .load()

        logger.info(f"Tabla {full_table_id} leída correctamente desde BigQuery.")
        return df
    except Exception as error:
        logger.error("Error al leer de BigQuery: " + str(error))
        return None

# %%
spark = (
    SparkSession.builder
    .appName("Spark")
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.2") \
    .config("temporaryGcsBucket", "flight2015dataset-temp") \
    .getOrCreate()
)


spark.sparkContext.setLogLevel("INFO")
logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
logger.info("Log de ejemplo guardado en archivo y consola.")

# %%
# Dataset de BigQuery
bq_dataset = "Flight2015_prod"

# %%
# Rutas de archivos en GCS
read_avg_flight = "gs://flight2015dataset/Archivos_parquet/avg_flight/"
read_airline_in_airport = "gs://flight2015dataset/Archivos_parquet/airline_in_airport/"
read_flights_per_cancell = "gs://flight2015dataset/Archivos_parquet/flights_per_cancell/"
read_airport_notin_thelist = "gs://flight2015dataset/Archivos_parquet/airport_notin_thelist/"

# %%
# Lectura de Parquet
avg_flight = spark.read.parquet(read_avg_flight)
airline_in_airport = spark.read.parquet(read_airline_in_airport)
flights_per_cancell = spark.read.parquet(read_flights_per_cancell)
airport_notin_thelist = spark.read.parquet(read_airport_notin_thelist)

# %%
# Escritura en BigQuery
write_DB_bigquery(avg_flight, bq_dataset, "avg_flight")
write_DB_bigquery(airline_in_airport, bq_dataset, "airline_in_airport")
write_DB_bigquery(flights_per_cancell, bq_dataset, "flights_cancellation")
write_DB_bigquery(airport_notin_thelist, bq_dataset, "airport_notin_thelist")

# %%
# Lectura desde BigQuery para validación
read_DB_bigquery(bq_dataset, "avg_flight")
read_DB_bigquery(bq_dataset, "airline_in_airport")
read_DB_bigquery(bq_dataset, "flights_cancellation")
read_DB_bigquery(bq_dataset, "airport_notin_thelist")
