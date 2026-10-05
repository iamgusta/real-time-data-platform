import json
import os
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer


kafka_server = os.getenv("KAFKA_SERVER", "localhost:9092")
topico = os.getenv("KAFKA_TOPIC", "telemetria-veiculos")

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
clickhouse_user = os.getenv("CLICKHOUSE_USER", "usuario")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "senha123")
clickhouse_database = os.getenv("CLICKHOUSE_DATABASE", "telemetria")


consumer = Consumer({
    "bootstrap.servers": kafka_server,
    "group.id": "grupo-telemetria",
    "auto.offset.reset": "earliest"
})


clickhouse = clickhouse_connect.get_client(
    host=clickhouse_host,
    port=clickhouse_port,
    username=clickhouse_user,
    password=clickhouse_password,
    database=clickhouse_database
)


consumer.subscribe([topico])

print("Consumer iniciado. Aguardando eventos...")


try:
    while True:
        mensagem = consumer.poll(1.0)

        if mensagem is None:
            continue

        if mensagem.error():
            print(f"Erro ao receber mensagem: {mensagem.error()}")
            continue

        evento = json.loads(
            mensagem.value().decode("utf-8")
        )

        data_hora = datetime.fromisoformat(
            evento["data_hora"]
        )


        clickhouse.insert(
            "eventos_telemetria",
            [[
                evento["veiculo_id"],
                evento["velocidade"],
                evento["temperatura_motor"],
                evento["nivel_combustivel"],
                data_hora
            ]],
            column_names=[
                "veiculo_id",
                "velocidade",
                "temperatura_motor",
                "nivel_combustivel",
                "data_hora"
            ]
        )

        print(f"Evento salvo: {evento['veiculo_id']}")


        alertas = []


        if evento["velocidade"] > 80:
            alertas.append(
                ("velocidade_alta", evento["velocidade"])
            )


        if evento["temperatura_motor"] > 100:
            alertas.append(
                ("temperatura_alta", evento["temperatura_motor"])
            )


        if evento["nivel_combustivel"] < 15:
            alertas.append(
                ("combustivel_baixo", evento["nivel_combustivel"])
            )


        for tipo_alerta, valor in alertas:

            clickhouse.insert(
                "alertas_telemetria",
                [[
                    evento["veiculo_id"],
                    tipo_alerta,
                    valor,
                    data_hora
                ]],
                column_names=[
                    "veiculo_id",
                    "tipo_alerta",
                    "valor",
                    "data_hora"
                ]
            )

            print(
                f"ALERTA: {evento['veiculo_id']} - "
                f"{tipo_alerta} - {valor}"
            )


except KeyboardInterrupt:
    print("\nConsumer encerrado.")


finally:
    consumer.close()