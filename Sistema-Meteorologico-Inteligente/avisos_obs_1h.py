import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_json, struct, lit

# Configuración para PySpark y Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

# Función principal
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Generar_Avisos") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    kafka_bootstrap_servers = "localhost:9092"

    # Leer datos de los tópicos Kafka
    calculos_1h = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "calculos_1h") \
        .option("startingOffsets", "latest") \
        .load()

    aerodromos_topico = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "aerodromos_topico") \
        .option("startingOffsets", "latest") \
        .load()

    zonas_topico = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "zonas_topico") \
        .option("startingOffsets", "latest") \
        .load()

    # Esquemas de datos
    calculos_1h_schema = """
        CodigoOACI STRING, max_intensidad_viento DOUBLE, max_temperatura DOUBLE, min_temperatura DOUBLE,
        mean_temperatura DOUBLE, max_humedad_relativa DOUBLE, min_humedad_relativa DOUBLE, mean_humedad_relativa DOUBLE,
        max_punto_rocio DOUBLE, min_punto_rocio DOUBLE, min_presion DOUBLE, max_presion DOUBLE, max_visibilidad DOUBLE,
        min_visibilidad DOUBLE, hubo_precipitacion BOOLEAN, hubo_cumulonimbos BOOLEAN, cobertura_predominante STRING,
        start TIMESTAMP, end TIMESTAMP
    """
    aerodromos_schema = """
        CODMETEOALERTA STRING, NOMBRE STRING, OACI STRING, NivelServicio STRING, GestorATS STRING
    """
    zonas_schema = """
        CODIGO INT, NOMBRE_DE_LA_ZONA STRING, PROVINCIA STRING, TEMP_MAX_AMARILLO DOUBLE, TEMP_MAX_NARANJA DOUBLE,
        TEMP_MAX_ROJO DOUBLE, TEMP_MIN_AMARILLO DOUBLE, TEMP_MIN_NARANJA DOUBLE, TEMP_MIN_ROJO DOUBLE,
        RACHA_MAX_AMARILLO DOUBLE, RACHA_MAX_NARANJA DOUBLE, RACHA_MAX_ROJO DOUBLE
    """

    # Convertir los valores Kafka a JSON y aplicar los esquemas
    calculos_1h_df = calculos_1h.selectExpr("CAST(value AS STRING)").selectExpr(f"from_json(value, '{calculos_1h_schema}') as data").select("data.*")
    aerodromos_df = aerodromos_topico.selectExpr("CAST(value AS STRING)").selectExpr(f"from_json(value, '{aerodromos_schema}') as data").select("data.*")
    zonas_df = zonas_topico.selectExpr("CAST(value AS STRING)").selectExpr(f"from_json(value, '{zonas_schema}') as data").select("data.*")

    # Realizar los joins
    joined_df = calculos_1h_df \
        .join(aerodromos_df, calculos_1h_df["CodigoOACI"] == aerodromos_df["OACI"], "inner") \
        .join(zonas_df, aerodromos_df["CODMETEOALERTA"] == zonas_df["CODIGO"], "inner")

    # Generar avisos basados en los umbrales
    avisos_df = joined_df.withColumn(
        "NivelAvisoTemperatura",
        when(col("max_temperatura") >= col("TEMP_MAX_ROJO"), "ROJO")
        .when(col("max_temperatura") >= col("TEMP_MAX_NARANJA"), "NARANJA")
        .when(col("max_temperatura") >= col("TEMP_MAX_AMARILLO"), "AMARILLO")
        .otherwise(None)
    ).withColumn(
        "NivelAvisoViento",
        when(col("max_intensidad_viento") >= col("RACHA_MAX_ROJO"), "ROJO")
        .when(col("max_intensidad_viento") >= col("RACHA_MAX_NARANJA"), "NARANJA")
        .when(col("max_intensidad_viento") >= col("RACHA_MAX_AMARILLO"), "AMARILLO")
        .otherwise(None)
    ).filter(
        col("NivelAvisoTemperatura").isNotNull() | col("NivelAvisoViento").isNotNull()
    )

    # Seleccionar y estructurar los avisos
    avisos_structured = avisos_df.select(
        col("OACI").alias("CodigoOACI"),
        col("NOMBRE").alias("NombreAerodromo"),
        col("NOMBRE_DE_LA_ZONA").alias("Zona"),
        col("NivelAvisoTemperatura"),
        col("NivelAvisoViento"),
        col("max_temperatura"),
        col("max_intensidad_viento")
    ).withColumn(
        "MensajeAviso",
        when(col("NivelAvisoTemperatura").isNotNull(),
             lit("Aviso: Superación de umbral de temperatura en nivel ") + col("NivelAvisoTemperatura"))
        .when(col("NivelAvisoViento").isNotNull(),
             lit("Aviso: Superación de umbral de viento en nivel ") + col("NivelAvisoViento"))
    )

    # Escribir los avisos en Kafka
    kafka_output = avisos_structured.selectExpr("to_json(struct(*)) AS value")

    '''
    kafka_query = kafka_output.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", "avisos_obs_1h") \
        .option("checkpointLocation", "./checkpoints/avisos_obs_1h") \
        .start()

    # Escribir los avisos en el DataLake
    datalake_query = avisos_structured.writeStream \
        .format("parquet") \
        .option("path", "/path/to/datalake/avisos_obs_1h") \
        .option("checkpointLocation", "./checkpoints/avisos_obs_1h") \
        .start()

    # Esperar a que ambos procesos terminen
    kafka_query.awaitTermination()
    datalake_query.awaitTermination()
    '''
    query = kafka_output.selectExpr("to_json(struct(*)) AS value").writeStream \
	    .outputMode("append") \
	    .format("console") \
 	    .option("truncate", "false") \
	    .start()
     
    query.awaitTermination()


