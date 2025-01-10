import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from sklearn.ensemble import IsolationForest
import pandas as pd
from functools import partial

# Configurar PySpark para usar el Python del entorno virtual actual
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("Batch_Deteccion_Anomalias") \
    .config("spark.sql.parquet.int96AsTimestamp", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Ruta del DataLake
data_path = "c:/spark-datalake/calculos_24h"

# Leer datos Parquet
data = spark.read.parquet(data_path)

# Lista de nombres de columnas
column_names = data.schema.names

# Validar el DataFrame original
print("Número de registros en el DataFrame original (data):", data.count())
print("Esquema del DataFrame original (data):")
data.printSchema()

# Convertir las columnas start y end a StringType si es necesario
data = data.withColumn("start", col("start").cast("string"))
data = data.withColumn("end", col("end").cast("string"))


# Preparar los datos para el modelo Isolation Forest
features = [
    "max_intensidad_viento", "max_temperatura", "min_temperatura", "mean_temperatura",
    "max_humedad_relativa", "min_humedad_relativa", "mean_humedad_relativa",
    "max_punto_rocio", "min_punto_rocio", "min_presion", "max_presion",
    "max_visibilidad", "min_visibilidad"
]

assembler = VectorAssembler(inputCols=features, outputCol="features")
data_vectorized = assembler.transform(data)

# Función para detectar anomalías
def detect_anomalies_in_partition(partition_data, column_names):
    partition_list = list(partition_data)
    print("Número de registros en la partición:", len(partition_list))
    
    if not partition_list:
        return []  # No procesar particiones vacías

    # Convertir partición a Pandas DataFrame
    pdf = pd.DataFrame(partition_list, columns=column_names)

    if pdf.empty:
        print("La partición está vacía después de la conversión a Pandas.")
        return []

    # Entrenar el Isolation Forest con las características seleccionadas
    iso_forest = IsolationForest(contamination=0.05, random_state=42)
    pdf["anomaly"] = iso_forest.fit_predict(pdf[features])
    pdf["anomaly"] = pdf["anomaly"].map({1: "Normal", -1: "Anomaly"})
    
    # Devolver todas las columnas originales más la columna "anomaly"
    return pdf.to_dict(orient="records")

# Crear una versión parcial de la función
detect_anomalies_with_columns = partial(detect_anomalies_in_partition, column_names=column_names)

# Aplicar el modelo en cada partición y convertir los resultados a un DataFrame
data_rdd = data.rdd.mapPartitions(detect_anomalies_with_columns)
data_anomalies = spark.createDataFrame(data_rdd)

# Validar el DataFrame de salida
print("Esquema del DataFrame de salida (data_anomalies):")
data_anomalies.printSchema()

print("Número de registros en el DataFrame de salida (data_anomalies):", data_anomalies.count())

# Guardar resultados en el DataLake
output_path = "c:/spark-datalake/control_calidad_24h"
if data_anomalies.count() > 0:
    data_anomalies.write.mode("overwrite").partitionBy("CodigoOACI").parquet(output_path)
    print("Proceso completado. Resultados guardados en:", output_path)
else:
    print("El DataFrame de salida (data_anomalies) está vacío. No se escribió ningún archivo.")
