# %%
import yaml
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, FloatType


# %%
def write_DB (parquet_DF, table):
    try:
        parquet_DF.write.jdbc(
            url=jdbc_url,
            table=table,
            mode="overwrite",
            properties=properties
        )
    except Exception as error:
        logger.error("Error Func write DB .yaml:" + str(error))

# %%
try:
    spark = (
        SparkSession.builder
        .appName("Spark")
        .master("spark://spark-master:7077")
        .config("spark.driver.extraJavaOptions", r'-Dlog4j.configurationFile=file:/opt/ETL/log4j.properties')\
        .config("spark.jars", "/opt/postgre/jars/postgresql-42.7.4.jar") \
        .getOrCreate()
    )



    spark.sparkContext.setLogLevel("INFO")

    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

    logger.info("Log de ejemplo guardado en archivo y consola.")
except Exception as error:
    logger.error("Error Creacion SparkSession:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

try:
    #reading the config file
    #geting file path into a dictionary
    with open("/opt/spark/Config_file/Config.Yaml", "r") as file:
        config = yaml.safe_load(file)
except Exception as error:
    logger.error("Error ruta .yaml:" + str(error))

errores_detectados = []


# %%
#BD conection

try:
    jdbc_url = "jdbc:postgresql://postgres-spark-DB:5432/sparkdb"
    properties = {
        "user": config['sparkdb']['user'],
        "password": config['sparkdb']['password'],
        "driver": "org.postgresql.Driver"
    }
except Exception as error:
    logger.error("Error conection BD:" + str(error))



# %%
#Read parquet file
try:
    read_avg_flight = config['Parquet_file']['avg_flight']
    read_airline_in_airport = config['Parquet_file']['airline_in_airport']
    read_flights_per_cancell = config['Parquet_file']['flights_per_cancell']
    read_airport_notin_thelist = config['Parquet_file']['airport_notin_thelist']
except Exception as error:
    logger.error("Error read parquet:" + str(error))


try:
    avg_flight = spark.read.parquet(read_avg_flight)
    airline_in_airport =  spark.read.parquet(read_airline_in_airport)
    flights_per_cancell = spark.read.parquet(read_flights_per_cancell)
    airport_notin_thelist = spark.read.parquet(read_airport_notin_thelist)
except Exception as error:
    logger.error("Error read parquet:" + str(error))

# %%
#write parquet file into DB

write_DB(avg_flight, "avg_flight")
write_DB(airline_in_airport,"airline_in_airport")
write_DB(flights_per_cancell,"flights_cancellation")
write_DB(airport_notin_thelist,"airport_notin_thelist")


if errores_detectados:
    logger.error("Errores detectados, abortando ejecución")
    sys.exit(1)
else:
    logger.info("Sin errores detectados")
    sys.exit(0)



