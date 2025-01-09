# **Sistema Inteligente de Gestión de Información Meteorológica para Aeródromos**

## **Descripción del Proyecto**
Este proyecto implementa un sistema avanzado para la adquisición, procesamiento y análisis en tiempo real de datos meteorológicos en aeródromos. Utilizando técnicas de **Machine Learning**, **streaming de datos** y una arquitectura distribuida, el sistema optimiza la predicción meteorológica, mejora la seguridad en la aviación y automatiza la generación de informes.

El sistema está diseñado para integrarse con plataformas existentes y ofrece funcionalidades clave como:
- Adquisición automática de datos meteorológicos desde sensores de aeródromos.
- Control de calidad y validación de datos en tiempo real.
- Predicción de variables meteorológicas críticas.
- Generación de alertas ante fenómenos adversos.
- Visualización interactiva de datos mediante dashboards.


## **Características Principales**
- **Streaming en tiempo real**: Procesamiento masivo de datos meteorológicos con Apache Kafka y Spark.
- **Machine Learning y predicción**:
  - Modelos como Isolation Forest y Redes Neuronales.
  - Predicción de variables como viento, visibilidad y presión atmosférica.
- **Control de calidad**: Algoritmos avanzados para validar y filtrar datos meteorológicos.
- **Visualización interactiva**: Dashboards dinámicos para el análisis y toma de decisiones.


## **Requisitos**
### Dependencias
Asegúrate de instalar las siguientes bibliotecas antes de ejecutar el proyecto:
```plaintext
confluent-kafka==2.6.1
pyspark==3.5.3
tensorflow==2.12.0
scikit-learn==1.3.0
numpy>=1.21.0
pandas>=1.3.0
matplotlib>=3.4.0
seaborn>=0.11.0
plotly==5.22.0
```

## **Arquitectura del Sistema**
El sistema consta de los siguientes módulos principales:

1. **Adquisición de datos**:
   - Conexión a sensores meteorológicos de aeródromos utilizando Apache Kafka como sistema de streaming.
   - Lectura y almacenamiento temporal de datos como velocidad del viento, visibilidad y presión atmosférica.

2. **Procesamiento**:
   - Transformación y análisis en tiempo real utilizando Apache Spark.
   - Limpieza de datos y control de calidad mediante algoritmos como Isolation Forest.

3. **Predicción**:
   - Uso de modelos de machine learning para predecir variables meteorológicas críticas.
   - Algoritmos implementados incluyen:
     - Redes Neuronales (RNN) para predicción de series temporales.
     - Modelos de regresión para tendencias climáticas.

4. **Visualización y alertas**:
   - Generación de dashboards interactivos con herramientas como Plotly.
   - Automatización de alertas para fenómenos meteorológicos adversos mediante notificaciones integradas.

## **Cómo Usar**
### **Instalación**
1. Clona el repositorio en tu máquina local:
   ```bash
   git clone https://github.com/tu_usuario/Sistema-Meteorologico-Inteligente.git
   cd Sistema-Meteorologico-Inteligente
   ```

### **Instalación**
1. Crea un entorno virtual (opcional pero recomendado):
   ```bash
   python -m venv env
   source env/bin/activate     # Linux/MacOS
   env\Scripts\activate        # Windows
   ```

2. En el directorio del proyecto, ejecuta el siguiente comando para instalar todas las dependencias requeridas por el proyecto:

```bash
pip install -r requirements.txt
```

### **Ingestar los datos de observación meteorológica aeronáutica (METARS)**
Para ingestar los METARS que utilizaremos en el proyecto se debe ejecutar el siguiente script con el intérprete Python que hemos configurado:

```bash
python ingestador_metars.py
```

### **Ingestar los metadatos de aeródromos**
Para ingestar los metadatos de aeródromos que utilizaremos en el proyecto se debe ejecutar el siguiente script con el intérprete Python que hemos configurado:

```bash
python ingestador_aerodromos.py
```

### **Ingestar los datos de zonas de aviso del Plan Meteoalerta**
Para ingestar la información de las zonas de avisos del Plan Meteoalerta (https://www.aemet.es/es/eltiempo/prediccion/avisos/ayuda/) que utilizaremos en el proyecto se debe ejecutar el siguiente script con el intérprete Python que hemos configurado:

```bash
python ingestador_zonas.py
```

### **Limpieza de METARS**
Para realizar las tareas de limpieza de los METARS, evitar duplicados o aquellos que tienen campos esenciales faltantes, se debe ejecutar el script python

```bash
python limpiador_metars.py
```

### **Cálculo de agregaciones de 24h**
Para calcular las agregaciones y cálculos relativos a 24h se debe ejecutar el script python

```bash
python calculos_24h.py
```

### **Cálculo de agregaciones de 12h**
Para calcular las agregaciones y cálculos relativos a 12h se debe ejecutar el script python

```bash
python calculos_12h.py
```
### **Cálculo de agregaciones de 1h**
Para calcular las agregaciones y cálculos relativos a 1h se debe ejecutar el script python

```bash
python calculos_1h.py
```


