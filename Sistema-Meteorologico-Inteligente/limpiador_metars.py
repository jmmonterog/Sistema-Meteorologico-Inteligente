import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming_2.12:3.5.3,org.apache.spark:spark-sql_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell'

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, struct, max as spark_max, to_timestamp

def process_metars(kafka_bootstrap_servers, input_topic, output_topic, checkpoint_location, datalake_path):
    # Crear sesión Spark
    spark = SparkSession.builder \
        .appName("limpiador_metars") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
   	.config("spark.sql.session.timeZone", "UTC") \
        .config("spark.kafka.consumer.poll.ms", "5000") \
	.config("spark.streaming.backpressure.enabled", "true") \
	.config("spark.streaming.kafka.consumer.poll.ms", "5000") \
	.config("spark.streaming.kafka.maxRatePerPartition", "100") \
	.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    # Leer el stream desde el tópico Kafka
    metars_stream_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Convertir los valores de Kafka a JSON
    from pyspark.sql.functions import from_json
    from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, BooleanType

    schema = StructType() \
        .add("FechaHoraDifusion", StringType()) \
        .add("CodigoOACI", StringType()) \
        .add("Tipo", StringType()) \
        .add("EsAuto", BooleanType()) \
        .add("EsCorregido", BooleanType()) \
        .add("EsNil", BooleanType()) \
        .add("FechaHoraNominal", StringType()) \
        .add("DireccionViento", DoubleType()) \
        .add("IntensidadViento", DoubleType()) \
        .add("RachaViento", StringType()) \
        .add("Visibilidad", DoubleType()) \
        .add("AlcanceVisualEnPista", StringType()) \
        .add("TiempoSignificativo", StringType()) \
        .add("Nubosidad", StringType()) \
        .add("Temperatura", DoubleType()) \
        .add("PuntoRocio", DoubleType()) \
        .add("Presion", DoubleType()) \
        .add("Precipitacion", StringType()) \
        .add("Trend", StringType()) \
        .add("InformacionReciente", StringType())

    metars_parsed = metars_stream_raw.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), schema).alias("data")) \
        .select("data.*")

    # Convertir las fechas a tipo Timestamp
    data_with_timestamps = metars_parsed \
        .withColumn("FechaHoraDifusion", to_timestamp(col("FechaHoraDifusion"), "yyyy-MM-dd'T'HH:mm")) \
        .withColumn("FechaHoraNominal", to_timestamp(col("FechaHoraNominal"), "yyyy-MM-dd'T'HH:mm"))

    # Filtrar los METARs según las condiciones especificadas
    metars_stream_filtered = data_with_timestamps.filter(
        (col("Tipo").isin("METAR", "SPECI")) &
        col("EsAuto").isNotNull() &
        col("EsCorregido").isNotNull() &
        col("CodigoOACI").isNotNull() &
        col("FechaHoraDifusion").isNotNull() &
        col("FechaHoraNominal").isNotNull() &
        col("Temperatura").isNotNull() &
        col("PuntoRocio").isNotNull() &
        col("Presion").isNotNull() &
        (~col("EsNil"))
    )

    # Añadir columnas eventTime, processingTime y customWatermark
    metars_stream_with_times = metars_stream_filtered \
        .withColumn("eventTime", col("FechaHoraDifusion")) \
        .withColumn("processingTime", current_timestamp()) \
        .withColumn("customWatermark", col("FechaHoraNominal") + expr("INTERVAL 29 MINUTES")) \
        .withColumn("Visibilidad", expr("CASE WHEN Visibilidad = -1.0 THEN 9999.0 ELSE Visibilidad END"))


    # Tratamiento de tardíos y duplicados
    metars_stream_cleansed = metars_stream_with_times \
         .withWatermark("eventTime", "29 minutes") \
         .filter(col("eventTime") <= col("customWatermark")) \
	 .dropDuplicates(["codigoOACI", "FechaHoraNominal"])


    # Escribir el resultado al DataLake en formato Parquet 
    metars_stream_to_datalake = metars_stream_cleansed.writeStream \
	.format("parquet") \
        .outputMode("append") \
        .partitionBy("CodigoOACI") \
	.option("path", f"{datalake_path}/metars_limpios") \
        .option("checkpointLocation", f"{checkpoint_location}/parquet_limpiador_metars") \
        .start()

    # Escribir el resultado al tópico Kafka
    query = metars_stream_cleansed.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .outputMode("Update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", f"{checkpoint_location}/kafka_limpiador_metars") \
        .start()

    # Esperar la terminación de ambos streams
    metars_stream_to_datalake.awaitTermination()
    query.awaitTermination()
    #spark.streams.awaitAnyTermination()

process_metars('localhost:9092', 'metars_topico', 'metars_limpios_topico', "file:///C:/spark-checkpoint", "file:///C:/spark-datalake")


