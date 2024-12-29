import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit, when
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType
from transformers import pipeline

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

# Crear resúmenes a partir de los datos
def preparar_datos_para_informe(row):
    """Toma una fila y crea un resumen basado en los datos meteorológicos."""
    resumen = []
    resumen.append(f"El día con inicio {row['start']} y fin {row['end']} en el aeródromo {row['CodigoOACI']} se registraron las siguientes condiciones:")

    # Intensidad del viento
    if row['max_intensidad_viento'] > 20:
        resumen.append(f"Hubo vientos fuertes con una intensidad máxima de {row['max_intensidad_viento']} km/h.")
    else:
        resumen.append(f"Los vientos fueron moderados, alcanzando un máximo de {row['max_intensidad_viento']} km/h.")

    # Nubosidad
    resumen.append(f"La cobertura nubosa predominante fue '{row['cobertura_predominante']}'.")

    # Precipitación
    if row['hubo_precipitacion']:
        resumen.append("Se registraron precipitaciones durante el periodo.")
    else:
        resumen.append("No se registraron precipitaciones durante el periodo.")

    # Temperatura
    resumen.append(f"La temperatura osciló entre {row['min_temperatura']}°C y {row['max_temperatura']}°C, con una media de {row['mean_temperatura']}°C.")

    # Visibilidad
    resumen.append(f"La visibilidad máxima alcanzada fue de {row['max_visibilidad']} metros.")

    return " ".join(resumen)

# Crear una función UDF para generar los resúmenes
def generar_resumen(row):
    return preparar_datos_para_informe(row.asDict())

generate_summary_udf = spark.udf.register("generar_resumen", generar_resumen, StringType())
data = data.withColumn("Resumen", generate_summary_udf(struct([col(c) for c in data.columns])))

# Generar informes detallados con NLP
generator = pipeline("text2text-generation", model="facebook/bart-large-cnn")

def generar_informe(resumen):
    """Genera un informe detallado a partir de un resumen usando NLP."""
    result = generator(resumen, max_length=200, min_length=50, do_sample=False)
    return result[0]['generated_text']

# Crear una función UDF para los informes
def generar_informe_udf(row):
    return generar_informe(row)

generate_report_udf = spark.udf.register("generar_informe", generar_informe_udf, StringType())
data = data.withColumn("Informe", generate_report_udf(col("Resumen")))

# Publicar los informes en el tópico Kafka informes_meteorologicos_topico
result_stream = data.select(to_json(struct([col(c) for c in data.columns])).alias("value"))
result_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "informes_meteorologicos_topico") \
    .option("checkpointLocation", "./checkpoints/informes") \
    .start()

# Ejecutar el proceso de streaming
spark.streams.awaitAnyTermination()
