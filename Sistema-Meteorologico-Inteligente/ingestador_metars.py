import csv
import json
from time import sleep
from confluent_kafka import Producer

# Ruta al archivo CSV
csv_file_path = "../data/INPUT_METARS.csv"

# Configuración del productor de Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto a la dirección de tu clúster Kafka
    'client.id': 'metar_producer',
    'queue.buffering.max.messages': 1000000,  # Incrementa el límite de mensajes en el buffer
    'queue.buffering.max.kbytes': 1048576,  # Incrementa el tamaño del buffer a 1 GB
    'queue.buffering.max.ms': 100,  # Tiempo máximo en ms que un mensaje puede esperar en el buffer
}

# Nombre del tópico
kafka_topic = "metars_topico"

# Crear el productor de Kafka
producer = Producer(producer_config)

# Función de callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar el mensaje: {err}")

# Leer y publicar los datos del CSV
def publish_metars(csv_file_path, kafka_topic):
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=';')

            for row in reader:
                # Convertir el registro a JSON
                metar_record = {
                    "FechaHoraDifusion": row["FechaHoraDifusion"],
                    "CodigoOACI": row["CodigoOACI"],
                    "Tipo": row["Tipo"],
                    "EsAuto": row["EsAuto"].lower() == 'true',
                    "EsCorregido": row["EsCorregido"].lower() == 'true',
                    "EsNil": row["EsNil"].lower() == 'true',
                    "FechaHoraNominal": row["FechaHoraNominal"],
                    "DireccionViento": float(row["DireccionViento"]) if row["DireccionViento"] else None,
                    "IntensidadViento": float(row["IntensidadViento"]) if row["IntensidadViento"] else None,
                    "RachaViento": row["RachaViento"],
                    "Visibilidad": float(row["Visibilidad"]) if row["Visibilidad"] else None,
                    "AlcanceVisualEnPista": row["AlcanceVisualEnPista"],
                    "TiempoSignificativo": row["TiempoSignificativo"],
                    "Nubosidad": row["Nubosidad"],
                    "Temperatura": float(row["Temperatura"]) if row["Temperatura"] else None,
                    "PuntoRocio": float(row["PuntoRocio"]) if row["PuntoRocio"] else None,
                    "Presion": float(row["Presion"]) if row["Presion"] else None,
                    "Precipitacion": row["Precipitacion"],
                    "Trend": row["Trend"],
                    "InformacionReciente": row["InformacionReciente"]
                }

                # Convertir el registro a una cadena JSON
                metar_json = json.dumps(metar_record)

                # Publicar el mensaje en Kafka
                producer.poll(0)
                producer.produce(
                    kafka_topic,
                    key=row["CodigoOACI"],  # Usar el código OACI como clave del mensaje
                    value=metar_json,
                    callback=delivery_report
                )
                sleep(0.01) # Pausa de 10 ms
            
            # Esperar a que todos los mensajes se envíen
            producer.flush()

    except Exception as e:
        print(f"Error al procesar el archivo CSV: {e}")

# Ejecutar la publicación de METARs
if __name__ == "__main__":
    publish_metars(csv_file_path, kafka_topic)