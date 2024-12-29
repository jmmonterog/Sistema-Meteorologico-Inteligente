import csv
import json
from time import sleep
from confluent_kafka import Producer

def read_csv_and_publish_to_kafka(csv_file_path, kafka_config, kafka_topic):
    """
    Lee un archivo CSV, transforma los registros a formato JSON y los publica en un tópico Kafka.

    Args:
        csv_file_path (str): Ruta al archivo CSV con los datos de los aeródromos.
        kafka_config (dict): Configuración del cliente Kafka.
        kafka_topic (str): Nombre del tópico Kafka donde se publicarán los datos.
    """
    # Crear un productor de Kafka
    producer = Producer(kafka_config)

    try:
        # Leer el archivo CSV
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=';')
            for row in csv_reader:
                try:
                    # Convertir la fila a JSON
                    record_json = json.dumps(row)

                    # Publicar en Kafka
                    producer.produce(kafka_topic, value=record_json)
                    producer.poll(0)  # Procesar eventos pendientes

                    print(f"Publicado en Kafka: {record_json}")
                    sleep(0.01)  # Pausa para evitar saturar Kafka

                except Exception as e:
                    print(f"Error al procesar la fila: {row} - {e}")

    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")

    finally:
        # Asegurarse de que todos los mensajes pendientes se envíen
        producer.flush()

if __name__ == "__main__":
    # Configuración de Kafka
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',  # Cambiar si es necesario
        'client.id': 'ingestador_aerodromos',
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.kbytes': 1048576,
        'queue.buffering.max.ms': 100
    }

    # Configuración del archivo CSV y el tópico Kafka
    csv_file_path = "aerodromos.csv"  # Cambiar a la ruta del archivo CSV
    kafka_topic = "aerodromos_topico"

    # Ejecutar el ingestador
    read_csv_and_publish_to_kafka(csv_file_path, kafka_config, kafka_topic)
