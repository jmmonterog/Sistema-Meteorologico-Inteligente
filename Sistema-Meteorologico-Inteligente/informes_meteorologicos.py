import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, udf
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType
from transformers import pipeline
import sys

# Configurar PySpark para usar el Python del entorno virtual actual
os.environ['PYSPARK_PYTHON'] = sys.executable  # Intérprete Python para ejecutores
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable  # Intérprete Python para el driver

# Configuración de PySpark
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("Generar_Informes_Meteorologicos_NLP") \
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

# Cargar el modelo de NLP preentrenado
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

# Función para generar resúmenes utilizando el modelo NLP
def generar_resumen_nlp(codigo_oaci, start, end, viento, temperatura, precipitacion, cobertura):
    texto_base = (
        f"El día con inicio {start} y fin {end} en el aeródromo {codigo_oaci} se registraron las siguientes condiciones: "
        f"viento máximo de {viento} km/h, temperaturas entre {temperatura['min']}°C y {temperatura['max']}°C "
        f"con una media de {temperatura['mean']}°C, "
        f"cobertura nubosa predominante '{cobertura}', "
        f"y {'hubo' if precipitacion else 'no hubo'} precipitaciones."
    )
    resumen = summarizer(texto_base, max_length=50, min_length=25, do_sample=False)
    return resumen[0]['summary_text']

# Registrar la función como UDF
@udf(returnType=StringType())
def generar_resumen_udf(codigo_oaci, start, end, max_intensidad_viento, min_temperatura, max_temperatura, mean_temperatura, hubo_precipitacion, cobertura_predominante):
    temperatura = {
        "min": min_temperatura,
        "max": max_temperatura,
        "mean": mean_temperatura
    }
    return generar_resumen_nlp(codigo_oaci, start, end, max_intensidad_viento, temperatura, hubo_precipitacion, cobertura_predominante)

# Agregar el resumen generado al DataFrame
data = data.withColumn(
    "Resumen",
    generar_resumen_udf(
        col("CodigoOACI"), col("start"), col("end"), col("max_intensidad_viento"),
        col("min_temperatura"), col("max_temperatura"), col("mean_temperatura"),
        col("hubo_precipitacion"), col("cobertura_predominante")
    )
)

# Publicar resúmenes en el tópico Kafka resúmenes_meteorologicos
data.select(to_json(struct([col("CodigoOACI"), col("Resumen")])).alias("value")) \
    .writeStream \
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
