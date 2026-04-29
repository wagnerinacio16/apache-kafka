from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
    filename="producer.log"
)

logger = logging.getLogger(__name__)

#Iniciando o Faker e o KafkaProducer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), #Serealizando os dados em JSON
    key_serializer=lambda v: json.dumps(v).encode('utf-8') #Serealizando a chave em JSON
)

def generate_temperature_data():
    return {
        'sensor_id': fake.uuid4(),
        'timestamp': fake.date_time().isoformat(), 
        'temperature': round(random.uniform(-20.0, 50.0), 2)
    }


if __name__ == "__main__":
    topic = 'temperature_sensor_topic'
    while True:
        try:
            data = generate_temperature_data()
            producer.send(topic=topic, key=data['sensor_id'], value=data)
            logger.info(f"Enviado: {data}")
            time.sleep(1)  # Envia um dado a cada segundo
        except Exception as e:
            logger.error(f"Erro ao enviar dados: {e}")