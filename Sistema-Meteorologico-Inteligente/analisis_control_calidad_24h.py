import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("Analisis_Resultados_IsolationForest") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Ruta de los resultados guardados
input_path = "c:/spark-datalake/control_calidad_24h"

# Leer los resultados guardados
data_anomalies = spark.read.parquet(input_path)

# 1.a Contar anomalías y registros normales
print("### Distribución de Anomalías y Registros Normales ###")
data_anomalies.groupBy("anomaly").count().show()

# 1.b Mostrar ejemplos de anomalías detectadas
print("### Ejemplo de Anomalías Detectadas ###")
data_anomalies.filter(col("anomaly") == "Anomaly").show(10, truncate=False)

# 2.a Visualizar proporción de anomalías vs registros normales
print("### Proporción de Anomalías vs Registros Normales ###")
data_anomalies.groupBy("anomaly").count().withColumn(
    "percentage", (col("count") / data_anomalies.count()) * 100
).show()

# 2.b Exportar a Pandas para visualización (opcional)
# Nota: Requiere que `features` sea serializable a Pandas si es un vector
print("### Exportar a Pandas para Visualización ###")
# Seleccionar columnas para exportar a Pandas
data_pd = data_anomalies.select(
    "CodigoOACI", "max_intensidad_viento", "max_temperatura", "min_temperatura", "anomaly"
).toPandas()

# Visualizar con Matplotlib (si estás en un entorno gráfico)
import matplotlib.pyplot as plt
import seaborn as sns

# Ajustar visualización según las dimensiones del vector de características
if not data_pd.empty:
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=data_pd, x="max_intensidad_viento", y="anomaly", hue="anomaly")
    plt.title("Distribución de Anomalías por Intensidad de Viento")
    plt.xlabel("Intensidad de Viento (Max)")
    plt.ylabel("Tipo de Registro")
    plt.legend(title="Anomaly")
    plt.show()
else:
    print("El DataFrame está vacío; no hay datos para visualizar.")
