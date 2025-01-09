import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import StructType, StringType, DoubleType, BooleanType
from pyspark.sql.types import StructField
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors
from sklearn.ensemble import IsolationForest
import numpy as np
import pandas as pd

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("Deteccion_Anomalias") \
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

# Leer datos desde Kafka
kafka_input_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "calculos_24h") \
    .option("startingOffsets", "earliest") \
    .load()

# Extraer y convertir los datos JSON
data = kafka_input_stream.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# Preparar los datos para el modelo Isolation Forest
features = [
    "max_intensidad_viento", "max_temperatura", "min_temperatura", "mean_temperatura",
    "max_humedad_relativa", "min_humedad_relativa", "mean_humedad_relativa",
    "max_punto_rocio", "min_punto_rocio", "min_presion", "max_presion",
    "max_visibilidad", "min_visibilidad"
]

assembler = VectorAssembler(inputCols=features, outputCol="features")
data_vectorized = assembler.transform(data)

# Convertir los datos a Pandas para Isolation Forest
def detect_anomalies_in_batch(iterator):
    batch_data = list(iterator)
    if len(batch_data) > 0:
        pdf = pd.DataFrame(batch_data, columns=features)
        iso_forest = IsolationForest(contamination=0.05, random_state=42)
        pdf["anomaly"] = iso_forest.fit_predict(pdf)
        pdf["anomaly"] = pdf["anomaly"].map({1: "Normal", -1: "Anomaly"})
        return pdf.values.tolist()
    return []


result_schema = StructType(data.schema.fields + [StructField("anomaly", StringType(), True)])
data_anomalies = data_vectorized.rdd.mapPartitions(detect_anomalies_in_batch).toDF(schema=result_schema)

# Publicar resultados en Kafka
result_stream = data_anomalies.select(to_json(struct([col(c) for c in data_anomalies.columns])).alias("value"))
result_stream.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "control_calidad") \
    .option("checkpointLocation", "./checkpoints/anomalies") \
    .start()

# Guardar resultados en el DataLake
result_stream.writeStream \
    .format("parquet") \
    .option("path", "./datalake/anomalies") \
    .option("checkpointLocation", "./checkpoints/datalake") \
    .start()

spark.streams.awaitAnyTermination()
