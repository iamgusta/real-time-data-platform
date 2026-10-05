import os

import clickhouse_connect
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


clickhouse = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "usuario"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "senha123"),
    database=os.getenv("CLICKHOUSE_DATABASE", "telemetria")
)


@app.get("/")
def inicio():
    return {
        "mensagem": "API de telemetria funcionando"
    }


@app.get("/eventos")
def listar_eventos():

    resultado = clickhouse.query("""
        SELECT
            veiculo_id,
            velocidade,
            temperatura_motor,
            nivel_combustivel,
            data_hora
        FROM eventos_telemetria
        ORDER BY data_hora DESC
        LIMIT 10
    """)

    eventos = []

    for linha in resultado.result_rows:
        eventos.append({
            "veiculo_id": linha[0],
            "velocidade": linha[1],
            "temperatura_motor": linha[2],
            "nivel_combustivel": linha[3],
            "data_hora": str(linha[4])
        })

    return eventos


@app.get("/alertas")
def listar_alertas():

    resultado = clickhouse.query("""
        SELECT
            veiculo_id,
            tipo_alerta,
            valor,
            data_hora
        FROM alertas_telemetria
        ORDER BY data_hora DESC
        LIMIT 10
    """)

    alertas = []

    for linha in resultado.result_rows:
        alertas.append({
            "veiculo_id": linha[0],
            "tipo_alerta": linha[1],
            "valor": linha[2],
            "data_hora": str(linha[3])
        })

    return alertas


@app.get("/resumo")
def resumo():

    resultado_eventos = clickhouse.query("""
        SELECT
            count(),
            uniqExact(veiculo_id),
            avg(velocidade),
            max(velocidade),
            avg(temperatura_motor)
        FROM eventos_telemetria
    """)

    resultado_alertas = clickhouse.query("""
        SELECT count()
        FROM alertas_telemetria
    """)

    dados = resultado_eventos.result_rows[0]

    return {
        "total_eventos": dados[0],
        "total_veiculos": dados[1],
        "velocidade_media": round(dados[2], 2),
        "maior_velocidade": round(dados[3], 2),
        "temperatura_media": round(dados[4], 2),
        "total_alertas": resultado_alertas.result_rows[0][0]
    }