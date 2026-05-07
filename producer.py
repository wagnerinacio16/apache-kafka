from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import logging
import uuid
from datetime import UTC, datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
    filename="producer.log"
)

logger = logging.getLogger(__name__)

#Iniciando o Faker e o KafkaProducer
fake = Faker('pt_BR')


def generate_product():    
    product = {
        "ean": fake.ean(length=13),
        "name": fake.word(),
        "description": fake.sentence(nb_words=10),
        "category": fake.word(),
        "brand": fake.company(),
        "price": round(random.uniform(10.0, 1000.0), 2)
        }
    return product

def generate_customer():
    custumer = {
        "customer_id": str(uuid.uuid4()),
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.estado_sigla()
    }
    return custumer

def generate_items():
    items = []
    num_items = random.randint(1, 4)

    for _ in range(num_items):
        product = generate_product()
        product["quantity"] = random.randint(1, 3)

        items.append(product)

    return items

def generate_event():
    items = generate_items()
    customer = generate_customer()

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["order_created", "payment_approved", "order_shipped", "order_delivered", "order_canceled"]),
        "event_timestamp": datetime.now(UTC).isoformat(),
        "source": "ecommerce_app",

        "customer": customer,

        "order": {
            "order_id": str(uuid.uuid4()),
            "status": "created",
            "payment_method": random.choice(["credit_card", "pix", "boleto"]),
            "total_amount": sum(item["quantity"] * item["price"] for item in items),
            "currency": "BRL",
            "items": items
        },

        "device": {
            "platform": random.choice(["mobile", "web"]),
            "app_version": f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}"
        }
    }

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'), #Serealizando os dados em JSON
    key_serializer=lambda v: json.dumps(v).encode('utf-8') #Serealizando a chave em JSON
)


if __name__ == "__main__":
   # print(generate_event())

    #print(json.dumps(generate_event(), indent=4, ensure_ascii=False))
    topic = 'ecommerce.orders.events'
    while True:
        try:
            data = generate_event()
            producer.send(topic=topic, key=data['event_id'], value=data)
            logger.info(f"Enviado: {data}")
            time.sleep(10)  # Envia um dado a cada segundo
        except Exception as e:
            logger.error(f"Erro ao enviar dados: {e}")