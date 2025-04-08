from kafka import KafkaConsumer
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()  # Carga las variables del archivo .env

postgresql_database = os.getenv("POSTGRESQL_DATABASE")
postgresql_user = os.getenv("POSTGRESQL_USER")
postgresql_host = os.getenv("POSTGRESQL_HOST")
postgresql_password = os.getenv("POSTGRESQL_PASSWORD")
postgresql_port = os.getenv("POSTGRESQL_PORT")

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
sasl_mechanism = os.getenv("SASL_MECHANISM")
sasl_plain_username = os.getenv("SASL_PLAIN_USERNAME")
sasl_plain_password = os.getenv("SASL_PLAIN_PASSWORD")

print('Connecting to PostgreSQL...')
try:
    conn = psycopg2.connect(database=postgresql_database, 
                            user=postgresql_user, 
                            host=postgresql_host,
                            password=postgresql_password,
                            port=postgresql_port)
    cur = conn.cursor()
    print("PostgreSQL connected successfully!")
except Exception as e:
    print(f"Could not connect to PostgreSQL: {e}")

consumer = KafkaConsumer('minecraft',
    bootstrap_servers=bootstrap_servers,
    security_protocol="SASL_SSL",
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password,
    auto_offset_reset="latest",
    enable_auto_commit=True
)

# Parseo de los datos recibidos y almacenamiento en PostgreSQL
for msg in consumer:
    try:
        # El valor de msg.value es un diccionario con clave '0' que contiene la cadena JSON
        record = json.loads(msg.value)  # Convierte el diccionario de Kafka a Python
        actual_message = json.loads(record['0'])  # Accedemos a la clave '0' y la deserializamos

        # Asumiendo que el record ahora es un diccionario con los datos correctos:
        ID = actual_message.get("ID")
        Nombre = actual_message.get("Nombre")
        Tipo = actual_message.get("Tipo")
        Rareza = actual_message.get("Rareza")
        Etapa_del_juego = actual_message.get("Etapa del juego")
        Dimension = actual_message.get("Dimensión")
        Usos_Principales = actual_message.get("Usos Principales")

        # Insertamos los datos en la base de datos PostgreSQL
        try:
            sql = """
            INSERT INTO minecraft (ID, Nombre, Tipo, Rareza, "Etapa del juego", Dimensión, "Usos Principales")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (ID, Nombre, Tipo, Rareza, Etapa_del_juego, Dimension, Usos_Principales))
            conn.commit()
            print(f"Registro insertado: {Nombre}")
        except Exception as e:
            print(f"Error al insertar en PostgreSQL: {e}")
            conn.rollback()
    except Exception as e:
        print(f"Error al procesar el mensaje de Kafka: {e}")
