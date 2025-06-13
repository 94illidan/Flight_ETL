# %%
import sys
import yaml
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, count, broadcast, desc, asc, when, rank, round
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, FloatType

# %%
def control_null (dataframe): 
    for dfcol in dataframe.columns:
        df_null = dataframe.filter(col(dfcol).isNull()).count()
        if df_null > 0:
            print(f"{dfcol} : {df_null} null")
        else:
            print(f"{dfcol} no tiene null")

errores_detectados = []
# %%
try:
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
except Exception as error:
    logger.error("Error Creacion SparkSession:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))


# %%
try:
    #reading the config file
    #geting file path into a dictionary
    with open("/opt/spark/Config_file/Config.Yaml", "r") as file:
        config = yaml.safe_load(file)
except Exception as error:
    logger.error("Error ruta .yaml:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))
    
try:
    read_parquet_airline = config["Parquet_file"]["df_airline"]
    read_parquet_flights = config["Parquet_file"]["df_flights"]
    read_parquet_airports = config["Parquet_file"]["df_airports"]
except Exception as error:
    logger.error("Error ruta .yaml:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

# %%
try:
    df_flights = spark.read.parquet(read_parquet_flights)
    df_airline = spark.read.parquet(read_parquet_airline)
    df_airports = spark.read.parquet(read_parquet_airports)
except Exception as error:
    logger.error("Error read.parquet:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

# %%
try:
    #avg per fly
    avg_flight = df_flights.join(broadcast(df_airline), df_flights.AIRLINE == df_airline.IATA_CODE) \
                        .groupBy(df_flights.AIRLINE, df_airline.AIRLINE) \
                        .agg(avg(df_flights.DISTANCE).alias("avg_DISTANCE")) \
                        .orderBy(desc("avg_DISTANCE")) \
                        .select(df_airline.AIRLINE.alias("AIRLINE_Name"), "avg_DISTANCE")
except Exception as error:
    logger.error("Error variable avg_flight:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

# %%
try:
    #how many times a flight visit an airport
    airline_in_airport = df_flights.join(broadcast(df_airports), df_flights.ORIGIN_AIRPORT == df_airports.IATA_CODE) \
                                .join(broadcast(df_airline), df_flights.AIRLINE == df_airline.IATA_CODE) \
                                .repartition(df_flights.DESTINATION_AIRPORT) \
                                .groupBy(df_flights.DESTINATION_AIRPORT, df_airline.AIRLINE) \
                                    .agg(count(df_flights.AIRLINE).alias("Count_visit_per_airline")) \
                                .orderBy(asc(df_flights.DESTINATION_AIRPORT)) \
                                .select(df_flights.DESTINATION_AIRPORT, df_airline.AIRLINE,"Count_visit_per_airline")
except Exception as error:
    logger.error("Error variable airline_in_airport:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

try:
    window_spec = Window.partitionBy("DESTINATION_AIRPORT").orderBy(desc("Count_visit_per_airline"))
    
    Rank_airline_in_airport = airline_in_airport.withColumn("ranking", rank().over(window_spec))


except Exception as error:
    logger.error("Error Rank_airline_in_airport:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))


# %%
try:
    #mention how many times an airline visit a destination and canceled this arrival
    flights_per_cancell = df_flights.select(col("AIRLINE"), col("CANCELLED"), col("DESTINATION_AIRPORT")) \
                                .filter(col("CANCELLED") == "1") \
                                .repartition(col("AIRLINE"), col("DESTINATION_AIRPORT")) \
                                .groupBy(col("AIRLINE"), col("DESTINATION_AIRPORT")) \
                                    .agg(count(col("AIRLINE")).alias("count_airlines_cancel")) \
                                .orderBy(desc("DESTINATION_AIRPORT"))
except Exception as error:
    logger.error("Error variable flights_per_cancell:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))

# %%
try:
    #i create this variable because i need to know how many airports have flights but are not in the list of airports
    airport_notin_thelist = df_flights.join(broadcast(df_airports), df_flights.DESTINATION_AIRPORT == df_airports.IATA_CODE, "left_anti") \
                                        .select("DESTINATION_AIRPORT") \
                                        .repartition("DESTINATION_AIRPORT") \
                                        .groupBy("DESTINATION_AIRPORT").agg(count("*").alias("Count_airports"))
except Exception as error:
    logger.error("Error variable airport_notin_thelist:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))
    
#Create parquet files
try:
    avg_flight.write.parquet(config["Parquet_file"]["avg_flight"], mode="overwrite")

    Rank_airline_in_airport.write.parquet(config["Parquet_file"]["airline_in_airport"], mode="overwrite")

    flights_per_cancell.write.parquet(config["Parquet_file"]["flights_per_cancell"], mode="overwrite")

    airport_notin_thelist.write.parquet(config["Parquet_file"]["airport_notin_thelist"], mode="overwrite")
except Exception as error:
    logger.error("Error write airport_notin_thelist:" + str(error))
    logger.error(traceback.format_exc())
    errores_detectados.append(str(e))


if errores_detectados:
    logger.error("Errores detectados, abortando ejecución")
    sys.exit(1)
else:
    logger.info("Sin errores detectados")
    sys.exit(0)