# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, count, broadcast, desc, asc, when, rank, round
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, FloatType

# %%
spark = (
    SparkSession.builder
    .appName("Spark")
    .config("spark.driver.extraJavaOptions", r'-Dlog4j.configurationFile=file:/home/illidan/proyecto_desde0/ETL/log4j.properties')\
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)

logger.info("Log de ejemplo guardado en archivo y consola.")

errores_detectados = []


# %%
try:
  read_parquet_airline = "gs://flight2015dataset/Archivos_parquet/df_airline/"
  read_parquet_flights =  "gs://flight2015dataset/Archivos_parquet/df_flights/"
  read_parquet_airports =  "gs://flight2015dataset/Archivos_parquet/df_airports/"

except Exception as error:
    logger.error("Error ruta .yaml:" + str(error))


# %%
try:
    df_flights = spark.read.parquet(read_parquet_flights)
    df_airline = spark.read.parquet(read_parquet_airline)
    df_airports = spark.read.parquet(read_parquet_airports)
except Exception as error:
    logger.error("Error read.parquet:" + str(error))

# %%
try:
    #avg per fly
    avg_flight = df_flights.join(broadcast(df_airline), df_flights.AIRLINE == df_airline.IATA_CODE) \
                        .groupBy(df_flights.AIRLINE, df_airline.AIRLINE) \
                        .agg(round(avg(df_flights.DISTANCE), 2).alias("avg_DISTANCE")) \
                        .orderBy(desc("avg_DISTANCE")) \
                        .select(df_airline.AIRLINE.alias("AIRLINE_Name"), "avg_DISTANCE")
except Exception as error:
    logger.error("Error variable avg_flight:" + str(error))


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

#a Rank of wich airline is the most visited in each airport
try:
    window_spec = Window.partitionBy("DESTINATION_AIRPORT").orderBy(desc("Count_visit_per_airline"))
    
    Rank_airline_in_airport = airline_in_airport.withColumn("ranking", rank().over(window_spec))


except Exception as error:
    logger.error("Error Rank_airline_in_airport:" + str(error))


# %%
try:
    #mention how many times an airline visit a destination and canceled this arrival
    flights_per_cancell = df_flights.select(col("AIRLINE"), col("CANCELLED"), col("DESTINATION_AIRPORT")) \
                                .filter(col("CANCELLED") == "1") \
                                .repartition(col("AIRLINE"), col("DESTINATION_AIRPORT")) \
                                .groupBy(col("AIRLINE"), col("DESTINATION_AIRPORT")) \
                                    .agg(count(col("AIRLINE")).alias("count_airlines_cancel")) \
                                .orderBy(desc("DESTINATION_AIRPORT")) \
                                .limit(100)
except Exception as error:
    logger.error("Error variable flights_per_cancell:" + str(error))


# %%
try:
    #i create this variable because i need to know how many airports have flights but are not in the list of airports
    airport_notin_thelist = df_flights.join(broadcast(df_airports), df_flights.DESTINATION_AIRPORT == df_airports.IATA_CODE, "left_anti") \
                                        .select("DESTINATION_AIRPORT") \
                                        .repartition("DESTINATION_AIRPORT") \
                                        .groupBy("DESTINATION_AIRPORT").agg(count("*").alias("Count_airports"))
except Exception as error:
    logger.error("Error variable airport_notin_thelist:" + str(error))


# %%
#Create parquet files

#variable for write parquet files

path_avg_flight = "gs://flight2015dataset/Archivos_parquet/avg_flight"
path_airline_in_airport = "gs://flight2015dataset/Archivos_parquet/airline_in_airport"
path_flights_per_cancell = "gs://flight2015dataset/Archivos_parquet/flights_per_cancell"
path_airport_notin_thelist = "gs://flight2015dataset/Archivos_parquet/airport_notin_thelist"

try:
    avg_flight.write.parquet(path_avg_flight, mode="overwrite")

    Rank_airline_in_airport.write.parquet(path_airline_in_airport, mode="overwrite")

    flights_per_cancell.write.parquet(path_flights_per_cancell, mode="overwrite")

    airport_notin_thelist.write.parquet(path_airport_notin_thelist, mode="overwrite")
except Exception as error:
    logger.error("Error write parquet files:" + str(error))


