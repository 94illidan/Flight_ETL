from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("S3ToGCSFoldered").getOrCreate()

# Subcarpetas a copiar (podés agregar más si hace falta)
folders = ["df_airline"]
#, "df_airports", "df_flights"]

# Ruta base S3 y GCS
s3_base = "s3a://matheurs-s3-test-gcp-integrations-1d4w1r1s/"
gcs_base = "gs://flight2015dataset/prueba_WSL/"

for folder in folders:
    s3_path = f"{s3_base}{folder}/"
    gcs_path = f"{gcs_base}{folder}/"

    print(f"Copiando: {s3_path} --> {gcs_path}")

    df = spark.read.parquet(s3_path)
    df.write.mode("overwrite").parquet(gcs_path)



