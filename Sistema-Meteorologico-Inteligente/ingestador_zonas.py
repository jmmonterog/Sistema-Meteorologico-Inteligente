import csv
import json
from time import sleep
from confluent_kafka import Producer

def leer_zonas_csv(file_path):
    """
    Lee el fichero CSV con las zonas Meteoalerta y lo convierte en una lista de diccionarios.
    """
    zonas = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            # Convertir los valores numéricos correctamente
            for key, value in row.items():
                if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
                    row[key] = int(value)
            zonas.append(row)
    return zonas

def publicar_en_kafka(zonas, kafka_topic, producer_config):
    """
    Publica los datos de las zonas en un tópico Kafka.
    """
    producer = Producer(producer_config)

    for zona in zonas:
        try:
            # Convertir la zona a JSON
            mensaje_json = json.dumps(zona)

            # Publicar en Kafka
            producer.poll(0)
            producer.produce(kafka_topic, value=mensaje_json)
            print(f"Publicado en Kafka: {mensaje_json}")

            # Pausa para evitar saturación
            sleep(0.01)
        except Exception as e:
            print(f"Error publicando la zona {zona}: {e}")

    producer.flush()

if __name__ == "__main__":
    # Configuración del productor Kafka
    producer_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'zonas_producer'
    }

    # Ruta del fichero CSV
    file_path = 'zonas_meteoalerta.csv'

    # Tópico Kafka de destino
    kafka_topic = 'zonas_topico'

    # Leer las zonas desde el CSV
    zonas = leer_zonas_csv(file_path)

    # Publicar las zonas en Kafka
    publicar_en_kafka(zonas, kafka_topic, producer_config)
