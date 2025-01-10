import os
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, SimpleRNN
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead
from pyspark.sql.window import Window

# Configurar PySpark para usar el Python del entorno virtual actual
os.environ['PYSPARK_PYTHON'] = sys.executable  # Intérprete Python para ejecutores
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable  # Intérprete Python para el driver

# Crear la sesión Spark
spark = SparkSession.builder \
    .appName("Prediccion_Lluvia") \
    .getOrCreate()

# Ruta del DataLake
data_path = "c:/spark-datalake/calculos_24h"

# Leer datos agrupados por CodigoOACI
data = spark.read.parquet(data_path)

# Crear la columna 'hubo_precipitacion_t+1'
window_spec = Window.partitionBy("CodigoOACI").orderBy("start")
data = data.withColumn("hubo_precipitacion_t+1", lead("hubo_precipitacion").over(window_spec))

# Filtrar filas donde 'hubo_precipitacion_t+1' es nulo (por ejemplo, últimos registros sin valor de día siguiente)
data = data.filter(col("hubo_precipitacion_t+1").isNotNull())

# Convertir a Pandas
data_pd = data.toPandas()

# --- Preprocesamiento de datos ---
selected_features = [
    'max_punto_rocio', 'min_punto_rocio', 'max_intensidad_viento',
    'min_temperatura', 'mean_temperatura', 'max_presion', 'max_temperatura'
]

X = data_pd[selected_features]
y = data_pd['hubo_precipitacion_t+1']

# Dividir los datos en conjunto de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Escalar los datos
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Aplicar SMOTE para balancear las clases en el conjunto de entrenamiento
smote = SMOTE(random_state=42)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train_scaled, y_train)

# --- Modelo 1: Regresión Logística ---
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train_resampled, y_train_resampled)
y_pred_lr = log_reg.predict(X_test_scaled)
print("Resultados de Regresión Logística:")
print(classification_report(y_test, y_pred_lr))

# --- Modelo 2: Bosques Aleatorios ---
random_forest = RandomForestClassifier(n_estimators=100, random_state=42)
random_forest.fit(X_train_resampled, y_train_resampled)
y_pred_rf = random_forest.predict(X_test_scaled)
print("Resultados de Bosques Aleatorios:")
print(classification_report(y_test, y_pred_rf))

# --- Modelo 3: Redes Neuronales Recurrentes (RNN) ---
# Reformatear los datos para RNN
X_train_rnn = X_train_resampled.reshape((X_train_resampled.shape[0], 1, X_train_resampled.shape[1]))
X_test_rnn = X_test_scaled.reshape((X_test_scaled.shape[0], 1, X_test_scaled.shape[1]))

# Crear la arquitectura de la RNN
rnn_model = Sequential()
rnn_model.add(SimpleRNN(50, activation='relu', input_shape=(1, X_train_resampled.shape[1])))
rnn_model.add(Dense(1, activation='sigmoid'))

# Compilar el modelo
rnn_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

# Entrenar el modelo
rnn_model.fit(X_train_rnn, y_train_resampled, epochs=20, batch_size=32, verbose=1)

# Evaluar el modelo
y_pred_rnn = (rnn_model.predict(X_test_rnn) > 0.5).astype(int).flatten()
print("Resultados de Redes Neuronales Recurrentes (RNN):")
print(classification_report(y_test, y_pred_rnn))

# --- Generar el DataFrame final ---
results = X_test.copy()
results['CodigoOACI'] = data_pd.loc[X_test.index, 'CodigoOACI']
results['DiaNatural'] = data_pd.loc[X_test.index, 'start'].dt.date
results['hubo_precipitacion'] = y_test.values
results['hubo_precipitacion_t+1'] = data_pd.loc[X_test.index, 'hubo_precipitacion_t+1']
results['prediccion_regresion_logistica'] = y_pred_lr.astype(int)
results['prediccion_bosques_aleatorios'] = y_pred_rf.astype(int)
results['prediccion_rnn'] = y_pred_rnn

# Convertir a PySpark DataFrame
results_spark = spark.createDataFrame(results)

# Escribir en Parquet agrupado por CodigoOACI
results_spark.write.mode("overwrite") \
    .partitionBy("CodigoOACI") \
    .parquet("c:/spark-datalake/prediccion_lluvia")

# Guardar en CSV
# Definir las columnas a incluir en el CSV
csv_columns = [
    'CodigoOACI', 'DiaNatural', 'max_punto_rocio', 'min_punto_rocio', 
    'max_intensidad_viento', 'min_temperatura', 'mean_temperatura', 
    'max_presion', 'max_temperatura', 'hubo_precipitacion', 'hubo_precipitacion_t+1',
    'prediccion_regresion_logistica', 'prediccion_bosques_aleatorios', 
    'prediccion_rnn'
]
output_csv_path = "c:/spark-datalake/predicciones.csv"
results.to_csv(output_csv_path, columns=csv_columns, sep=';', index=False)

print("Predicciones guardadas en formato Parquet agrupadas por CodigoOACI.")
