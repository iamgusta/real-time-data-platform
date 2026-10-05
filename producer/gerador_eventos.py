import json
import os
import random
import time
from datetime import datetime

from confluent_kafka import Producer


kafka_server = os.getenv("KAFKA_SERVER", "localhost:9092")
topico = os.getenv("KAFKA_TOPIC", "telemetria-veiculos")


producer = Producer({
    "bootstrap.servers": kafka_server
})


while True:
    evento = {
        "veiculo_id": f"VEICULO-{random.randint(1, 10):02}",
        "velocidade": round(random.uniform(0, 100), 2),
        "temperatura_motor": round(random.uniform(70, 110), 2),
        "nivel_combustivel": round(random.uniform(5, 100), 2),
        "data_hora": datetime.now().isoformat()
    }

    evento_json = json.dumps(evento)

    producer.produce(
        topico,
        value=evento_json
    )

    producer.flush()

    print(f"Evento enviado: {evento_json}")

    time.sleep(2)