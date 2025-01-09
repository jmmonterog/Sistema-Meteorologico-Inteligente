from pyspark.sql import SparkSession

def read_parquet(parquet_path):
    # Crear una sesión Spark
    spark = SparkSession.builder \
        .appName("LeerContenidoParquet") \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .getOrCreate()

    # Leer los archivos Parquet desde el directorio especificado
    df = spark.read.parquet(parquet_path)

    # Mostrar las primeras filas
    print("Contenido del Parquet:")
    df.show(truncate=False)

    # Imprimir el esquema
    print("Esquema del DataFrame:")
    df.printSchema()

# Ruta al directorio donde se guardan los archivos Parquet
parquet_path = "file:///C:/spark-datalake/metars_limpios"

# Llamar a la función para leer el contenido
read_parquet(parquet_path)

