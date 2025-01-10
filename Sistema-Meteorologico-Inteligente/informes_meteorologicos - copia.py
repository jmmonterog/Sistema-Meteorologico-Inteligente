import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, udf
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType
import pandas as pd
from transformers import pipeline
import sys

# Configurar PySpark para usar el Python del entorno virtual actual
os.environ['PYSPARK_PYTHON'] = sys.executable  # Intérprete Python para ejecutores
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable  # Intérprete Python para el driver

# Configuración de PySpark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("Generar_Informes_Meteorologicos") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Esquema de los datos JSON
schema = StructType() \
    .add("CodigoOACI", StringType()) \
    .add("max_intensidad_viento", DoubleType()) \
    .add("max_temperatura", DoubleType()) \
    .add("min_temperatura", DoubleType()) \
    .add("mean_temperatura", DoubleType()) \
    .add("max_humedad_relativa", DoubleType()) \
    .add("min_humedad_relativa", DoubleType()) \
    .add("mean_humedad_relativa", DoubleType()) \
    .add("max_punto_rocio", DoubleType()) \
    .add("min_punto_rocio", DoubleType()) \
    .add("min_presion", DoubleType()) \
    .add("max_presion", DoubleType()) \
    .add("max_visibilidad", DoubleType()) \
    .add("min_visibilidad", DoubleType()) \
    .add("hubo_precipitacion", BooleanType()) \
    .add("cobertura_predominante", StringType()) \
    .add("start", StringType()) \
    .add("end", StringType())

# Leer datos desde el tópico Kafka calculos_24h
kafka_input_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "calculos_24h") \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir los datos JSON
data = kafka_input_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Generar resúmenes en el DataFrame
@udf(returnType=StringType())
def generar_resumen(row):
    """Generar resumen directamente desde una fila."""
    resumen = []
    resumen.append(f"El día con inicio {row['start']} y fin {row['end']} en el aeródromo {row['CodigoOACI']} se registraron las siguientes condiciones:")
    if row['max_intensidad_viento'] > 20:
        resumen.append(f"Hubo vientos fuertes con una intensidad máxima de {row['max_intensidad_viento']} km/h.")
    else:
        resumen.append(f"Los vientos fueron moderados, alcanzando un máximo de {row['max_intensidad_viento']} km/h.")
    resumen.append(f"La cobertura nubosa predominante fue '{row['cobertura_predominante']}'.")
    if row['hubo_precipitacion']:
        resumen.append("Se registraron precipitaciones durante el periodo.")
    else:
        resumen.append("No se registraron precipitaciones durante el periodo.")
    resumen.append(f"La temperatura osciló entre {row['min_temperatura']}°C y {row['max_temperatura']}°C, con una media de {row['mean_temperatura']}°C.")
    resumen.append(f"La visibilidad máxima alcanzada fue de {row['max_visibilidad']} metros.")
    return " ".join(resumen)

data = data.withColumn("Resumen", generar_resumen(struct([col(c) for c in data.columns])))

# Publicar resúmenes en el tópico Kafka resúmenes_meteorologicos
resumen_stream = data.select(to_json(struct([col("CodigoOACI"), col("Resumen")])).alias("value"))
resumen_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "resumenes_meteorologicos") \
    .option("checkpointLocation", "c:/spark-checkpoint/kafka_resumenes") \
    .start()


# Guardar resúmenes en Parquet
output_path = "c:/spark-datalake/informes_meteorologicos"
data.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "c:/spark-checkpoint/parquet_resumenes") \
    .outputMode("append") \
    .partitionBy("CodigoOACI") \
    .start()

# Ejecutar el proceso de streaming
spark.streams.awaitAnyTermination()
