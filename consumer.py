from kafka import KafkaConsumer
import json
import logging

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S"
)

logger = logging.getLogger(__name__)


consumer = KafkaConsumer(
    'temperature_sensor_topic', #Nome do topico a ser consumido
    api_version=(3, 8, 0),
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest', #Le as mensagens desde o início do tópico
    enable_auto_commit=True, #Confirma automaticamente o consumo das mensagens
    group_id='clients_consumer_group', #Identificador do grupo de consumidores
    value_deserializer=lambda v: json.loads(v.decode('utf-8')), #Desserializando os dados de JSON
)

if __name__ == "__main__":
    try:
        topic = 'temperature_sensor_topic'
        consumer.subscribe([topic]) #Inscreve-se no tópico para consumir mensagens
        logger.info(f"Consumindo mensagens do tópico: {topic}")
        
        while True:
            for message in consumer:
                logger.info(f"Recebido: {message.value}")
                print(f"Recebido: {message.value}")
    except Exception as e:
        logger.error(f"Erro ao consumir dados: {e}")