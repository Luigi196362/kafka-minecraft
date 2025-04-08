from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
from dotenv import load_dotenv
import os
from fastapi import FastAPI
import threading

# Cargar variables de entorno
load_dotenv()

# Configuración de Kafka y MongoDB
bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
sasl_mechanism = os.getenv("SASL_MECHANISM")
sasl_plain_username = os.getenv("SASL_PLAIN_USERNAME")
sasl_plain_password = os.getenv("SASL_PLAIN_PASSWORD")
mongo_uri = os.getenv("MONGO_URI")

# Inicializar FastAPI
app = FastAPI()

def start_consumer():
    try:
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        db = client.minecraft
        print("MongoDB Connected successfully!")
    except Exception as e:
        print(f"Could not connect to MongoDB: {e}")
        return

    consumer = KafkaConsumer(
        'minecraft-mongo',
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism=sasl_mechanism,
        sasl_plain_username=sasl_plain_username,
        sasl_plain_password=sasl_plain_password,
        auto_offset_reset="latest",
        enable_auto_commit=True
    )

    for msg in consumer:
        try:
            record = json.loads(msg.value)
            print(record)
            meme_rec = {'data': record}
            result = db.minecraft.insert_one(meme_rec)
            print("Data inserted with record id", result.inserted_id)
        except Exception as e:
            print(f"Could not insert into MongoDB: {e}")

# Hilo que corre el consumer al iniciar FastAPI
@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_consumer, daemon=True).start()

# Endpoint opcional para saber que el servicio está vivo
@app.get("/")
def read_root():
    return {"status": "MongoDB Kafka consumer is running"}
