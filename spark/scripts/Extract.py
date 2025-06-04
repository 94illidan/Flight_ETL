# %%
import sys
import yaml
import traceback
from pyspark.sql import SparkSession


# %%
#built spark session
spark = (
    SparkSession.builder
    .appName("Spark")
    .master("spark://spark-master:7077")
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=/opt/ETL/log4j.properties")\
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")
logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)



logger.info("Log de ejemplo guardado en archivo y consola.")

errores_detectados = []
# %%

try:
    #geting file path into a dictionary
    with open("/opt/spark/Config_file/Config.Yaml", "r") as file:
        config = yaml.safe_load(file)





    #search for the path of the files
    path_vuelos = config["path"]["path_vuelos"]
    Path_aerolineas = config["path"]["Path_aerolineas"]
    path_aeropuertos = config["path"]["path_aeropuertos"]
    #archivos parquet
    path_df_flights = config["Parquet_file"]["df_flights"]
    path_df_airline = config["Parquet_file"]["df_airline"]
    path_df_airports = config["Parquet_file"]["df_airports"]
except Exception as error:
    logger.error("Error ruta .yaml:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(error))

# %%
try:
    
    df_flights = spark.read.csv(path_vuelos, header= True, inferSchema=True)
    df_airline = spark.read.csv(Path_aerolineas, header= True, inferSchema=True)
    df_airports = spark.read.csv(path_aeropuertos, header=True, inferSchema=True)

except Exception as error:
    logger.error("Error read.csv:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(error))
# %%
#Export into a parquet file

try:
    df_flights.write.parquet(path_df_flights, mode="overwrite")
    df_airline.write.parquet(path_df_airline, mode="overwrite")
    df_airports.write.parquet(path_df_airports, mode="overwrite")
except Exception as error:
    logger.error("Error write.parquet:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(error))



if errores_detectados:
    logger.error("Errores detectados, abortando ejecución")
    sys.exit(1)
else:
    logger.info("Sin errores detectados")
    sys.exit(0)
