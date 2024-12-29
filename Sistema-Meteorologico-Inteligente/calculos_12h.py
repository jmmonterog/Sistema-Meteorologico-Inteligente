import os

os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\AEMet\\anaconda3\\envs\\environment_uoc20232pec3-actual\\python.exe'  # Intérprete Python para ejecutores
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\AEMet\\anaconda3\\envs\\environment_uoc20232pec3-actual\\python.exe'  # Intérprete Python para el driver

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, expr, current_timestamp, lit, when, udf
from pyspark.sql.types import DoubleType
from pyspark.sql.types import BooleanType
from pyspark.sql.types import StringType
from pyspark.sql import functions as F  # Importación para el alias F

def process_metars(kafka_bootstrap_servers, input_topic, output_topic,checkpoint_location, datalake_path):
    spark = SparkSession.builder \
        .appName("Calculos_12h") \
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


    # Leer datos desde Kafka
    metars_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "earliest") \
	.option("failOnDataLoss", "false") \
        .load()

    # Convertir el valor de Kafka a JSON
    metars_parsed = metars_stream.selectExpr("CAST(value AS STRING) as json") \
        .selectExpr("CAST(json AS STRING)") \
        .selectExpr("""
            from_json(json, '
            FechaHoraDifusion STRING, 
            CodigoOACI STRING, 
            Tipo STRING, 
            EsAuto BOOLEAN, 
            EsCorregido BOOLEAN, 
            EsNil BOOLEAN, 
            FechaHoraNominal STRING, 
            DireccionViento DOUBLE, 
            IntensidadViento DOUBLE, 
            RachaViento STRING, 
            Visibilidad DOUBLE, 
            AlcanceVisualEnPista STRING, 
            TiempoSignificativo STRING, 
            Nubosidad STRING, 
            Temperatura DOUBLE, 
            PuntoRocio DOUBLE, 
            Presion DOUBLE, 
            Precipitacion STRING, 
            Trend STRING, 
            InformacionReciente STRING, 
            eventTime STRING, 
            processingTime STRING, 
            customWatermark STRING
            ') as data
        """).select("data.*")

    # Conversión de columnas y filtrado
    metars_filtered = metars_parsed \
        .withColumn("FechaHoraNominal", col("FechaHoraNominal").cast("timestamp")) \
        .filter(
            (col("Tipo").isin("METAR", "SPECI")) &
            (~col("EsNil")) &
            (col("EsAuto").isNotNull()) &
            (col("EsCorregido").isNotNull()) &
            (col("CodigoOACI").isNotNull()) &
            (col("FechaHoraNominal").isNotNull()) &
            (col("Temperatura").isNotNull()) &
            (col("PuntoRocio").isNotNull()) &
            (col("Presion").isNotNull())
        )

    # Reemplazar visibilidad -1.0 por 9999.0
    metars_filtered = metars_filtered.withColumn(
        "Visibilidad",
        when(col("Visibilidad") == -1, 9999).otherwise(col("Visibilidad"))
    )

    # Calcular humedad relativa
    def calculate_relative_humidity(temp, dew_point):
        if temp is None or dew_point is None:
            return None
        e_t = 6.11 * 10 ** (7.5 * temp / (237.7 + temp))
        e_td = 6.11 * 10 ** (7.5 * dew_point / (237.7 + dew_point))
        return 100 * (e_td / e_t)

    humidity_udf = udf(calculate_relative_humidity, DoubleType())
    metars_filtered = metars_filtered.withColumn("HumedadRelativa", humidity_udf(col("Temperatura"), col("PuntoRocio")))


    def cobertura_dominante(nubosidad):
        # Definir jerarquía de prioridades
        prioridad = {"FEW": 1, "SCT": 2, "BKN": 3, "OVC": 4}
        mapping = {
            "FEW": "pocas nubes",
            "SCT": "nubes dispersas",
            "BKN": "nubes fragmentadas",
            "OVC": "nublado total"
        }
        
        # Inicializar valores
        mejor_tipo = None
        mejor_prioridad = 0
    
        if nubosidad:
            # Iterar directamente sobre los elementos de la lista
            for grupo in nubosidad:
                tipo = grupo[:3]  # Extraer el tipo (e.g., "SCT" de "SCT031")
                if tipo in prioridad and prioridad[tipo] > mejor_prioridad:
                    mejor_tipo = tipo
                    mejor_prioridad = prioridad[tipo]
        
        # Retornar el texto descriptivo o 'despejado' si no hay nubosidad válida
        return mapping.get(mejor_tipo, "despejado")
    
    # Registrar la función como UDF
    cobertura_udf = udf(cobertura_dominante, StringType())
    

    precipitation_values = [
       "-RA", "RA", "+RA", "-SN", "SN", "+SN", "-DZ", "DZ", "+DZ",
       "-GR", "GR", "+GR", "-GS", "GS", "+GS", "UP", "RASN",
       "SHRA", "SHSN", "SHGR", "SHGS", "TSRA", "TSSN", "TSGR",
       "TSGS", "+TSRA", "-SHSN", "SHRA"
   ]
    
    def has_precipitation(precip):
        if precip is None:
            return False
        return any(value in precip.split() for value in precipitation_values)

    precip_udf = udf(has_precipitation, BooleanType())

    metars_filtered = metars_filtered.withColumn(
        "hubo_precipitacion",
        precip_udf(col("Precipitacion"))
    )

    # Determinar si hubo cumulonimbos o precipitación
    metars_filtered = metars_filtered.withColumn(
        "hubo_cumulonimbos",
        col("Nubosidad").like("%CB%")
    )
    
    # Agregar watermark antes de la agregación
    metars_filtered = metars_filtered.withWatermark("FechaHoraNominal", "1 day")
    
    

    # Agregaciones por ventana de 12 horas
    ventana_12h = metars_filtered.groupBy(
        col("CodigoOACI"),
        window(col("FechaHoraNominal"), "12 hour", "30 minute")
    ).agg(
        expr("MAX(IntensidadViento) AS max_intensidad_viento"),
        expr("MAX(Temperatura) AS max_temperatura"),
        expr("MIN(Temperatura) AS min_temperatura"),
        expr("AVG(Temperatura) AS mean_temperatura"),
        expr("MAX(HumedadRelativa) AS max_humedad_relativa"),
        expr("MIN(HumedadRelativa) AS min_humedad_relativa"),
        expr("AVG(HumedadRelativa) AS mean_humedad_relativa"),
        expr("MAX(PuntoRocio) AS max_punto_rocio"),
        expr("MIN(PuntoRocio) AS min_punto_rocio"),
        expr("MIN(Presion) AS min_presion"),
        expr("MAX(Presion) AS max_presion"),
        expr("MAX(Visibilidad) AS max_visibilidad"),
        expr("MIN(Visibilidad) AS min_visibilidad"),
        expr("MAX(hubo_precipitacion) AS hubo_precipitacion"),
        F.max("hubo_cumulonimbos").alias("hubo_cumulonimbos"),
        F.expr("COLLECT_LIST(Nubosidad) AS coberturas")
    ).withColumn(
        "cobertura_predominante", cobertura_udf(F.col("coberturas"))
    ).withColumn("start", col("window.start")) \
     .withColumn("end", col("window.end")) \
     .drop("window", "nubosidad_list", "coberturas")
     
    ventana_12h = ventana_12h.withColumn(
        "end", expr("end - INTERVAL 1 SECOND")
    )
    


    # Guardar resultados en el DataLake en formato Parquet
    result_to_datalake = ventana_12h.writeStream \
        .outputMode("Append") \
        .partitionBy("CodigoOACI") \
        .format("parquet") \
        .option("checkpointLocation",f"{checkpoint_location}/calculos_agregados_12h_02") \
	.trigger(processingTime="1 second") \
        .start(f"{datalake_path}/calculos_12h_02")

    # Escribir resultados al tópico Kafka calculos_12h
    result_to_kafka = ventana_12h.selectExpr("to_json(struct(*)) AS value").writeStream \
        .outputMode("Update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", f"{checkpoint_location}/calculos_agregados_12h_02") \
        .start()
    


    # Esperar terminación
    result_to_datalake.awaitTermination()
    result_to_kafka.awaitTermination()
    #spark.streams.awaitAnyTermination()


    '''
    # Escribir en consola
    query = ventana_12h.selectExpr("to_json(struct(*)) AS value").writeStream \
	    .outputMode("complete") \
	    .format("console") \
 	    .option("truncate", "false") \
	    .start()
     
    query.awaitTermination()
    '''
   
    
	

# Parámetros de configuración
kafka_bootstrap_servers = "localhost:9092"
input_topic = "metars_limpios_topico"
output_topic = "calculos_12h"

process_metars('localhost:9092', 'metars_limpios_topico', 'calculos_12h', 'C:\spark-checkpoint', 'C:\spark-datalake')

