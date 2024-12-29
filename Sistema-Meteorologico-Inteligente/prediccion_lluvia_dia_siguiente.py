import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, SimpleRNN

# --- 1. Preprocesamiento de datos ---
# Variables seleccionadas basadas en el análisis previo
selected_features = [
    'max_punto_rocio', 'min_punto_rocio', 'max_intensidad_viento',
    'min_temperatura', 'mean_temperatura', 'max_presion', 'max_temperatura'
]
X = df[selected_features]
y = df['hubo_precipitacion_t+1']

# Dividir los datos en conjunto de entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Escalar los datos
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Aplicar SMOTE para balancear las clases en el conjunto de entrenamiento
smote = SMOTE(random_state=42)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train_scaled, y_train)

# Verificar el balance de clases
print("Distribución de clases antes del balanceo:", y_train.value_counts())
print("Distribución de clases después del balanceo:", pd.Series(y_train_resampled).value_counts())

# --- 2. Modelo 1: Regresión Logística ---
print("\n--- Modelo 1: Regresión Logística ---")
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train_resampled, y_train_resampled)
y_pred_lr = log_reg.predict(X_test_scaled)
print("Resultados de Regresión Logística:")
print(classification_report(y_test, y_pred_lr))

# --- 3. Modelo 2: Bosques Aleatorios ---
print("\n--- Modelo 2: Bosques Aleatorios ---")
random_forest = RandomForestClassifier(n_estimators=100, random_state=42)
random_forest.fit(X_train_resampled, y_train_resampled)
y_pred_rf = random_forest.predict(X_test_scaled)
print("Resultados de Bosques Aleatorios:")
print(classification_report(y_test, y_pred_rf))

# --- 4. Modelo 3: Redes Neuronales Recurrentes (RNN) ---
print("\n--- Modelo 3: Redes Neuronales Recurrentes (RNN) ---")
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
y_pred_rnn = (rnn_model.predict(X_test_rnn) > 0.5).astype(int)
print("Resultados de Redes Neuronales Recurrentes (RNN):")
print(classification_report(y_test, y_pred_rnn))
