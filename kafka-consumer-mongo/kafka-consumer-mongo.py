from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
from dotenv import load_dotenv
import os

load_dotenv()  # Carga las variables del archivo .env

bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
sasl_mechanism = os.getenv("SASL_MECHANISM")
sasl_plain_username = os.getenv("SASL_PLAIN_USERNAME")
sasl_plain_password = os.getenv("SASL_PLAIN_PASSWORD")

mongo_uri = os.getenv("MONGO_URI")

uri=mongo_uri


try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.minecraft
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer('minecraft',
  bootstrap_servers=bootstrap_servers,
  security_protocol="SASL_SSL",
  sasl_mechanism=sasl_mechanism,
  sasl_plain_username=sasl_plain_username,
  sasl_plain_password=sasl_plain_password,
  auto_offset_reset="latest",
  enable_auto_commit=True,
  #consumer_timeout_ms=10000
)


# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    try:
       meme_rec = {'data': record }
       print (record)
       record_id = db.minecraft.insert_one(meme_rec)
       print("Data inserted with record ids", record_id)
    except:
       print("Could not insert into MongoDB")

