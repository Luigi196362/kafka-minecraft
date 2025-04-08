from fastapi import FastAPI
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os
import pandas as pd

# Cargar las variables de entorno
load_dotenv()

# Configuración de Kafka
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
sasl_mechanism = os.getenv("SASL_MECHANISM")
sasl_plain_username = os.getenv("SASL_PLAIN_USERNAME")
sasl_plain_password = os.getenv("SASL_PLAIN_PASSWORD")

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol="SASL_SSL",
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Crear la API con FastAPI
app = FastAPI()

# Enviar los datos a Kafka
def send_to_kafka(data: dict):
    producer.send('minecraft', value=data)
    producer.flush()  # Asegurarse de que el mensaje se envíe

# Endpoint que recibe el POST vacío
@app.post("/send_data/")
async def send_data():
    url = 'https://raw.githubusercontent.com/Luigi196362/spark-minecraft/main/results/dataPgsql.json'
    df = pd.read_json(url, orient='columns')

    # Enviar las primeras 3 filas del archivo
    for _, value in df.head(3).iterrows():
        dict_data = dict(value)
        send_to_kafka(dict_data)

    return {"message": "Data from file sent to Kafka successfully"}
