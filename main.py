import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import pprint

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

conn = psycopg2.connect(
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASS"],
    host=os.environ["DB_HOST"],
    port=os.environ["DB_PORT"]
)
cursor = conn.cursor()


def parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@app.post("/receber-dado")
async def receber_dado(request: Request):
    data = await request.json()
    pprint.pprint(data)  

    channel = data.get("channel", {})
    nome_canal = channel.get("name")
    latitude = parse_float(channel.get("latitude"))
    longitude = parse_float(channel.get("longitude"))

    for feed in data.get("feeds", []):
        created_at = feed.get("created_at")
        valor_ads_bateria = parse_float(feed.get("field1"))
        tensao_bateria = parse_float(feed.get("field2"))
        corrente_bateria = parse_float(feed.get("field3"))
        nivel_bateria = parse_float(feed.get("field4"))
        valor_ads_hs = parse_float(feed.get("field5"))
        tensao_hs = parse_float(feed.get("field6"))
        corrente_hs = parse_float(feed.get("field7"))
        nivel_agua = parse_float(feed.get("field8"))

        if None in [
            valor_ads_bateria, tensao_bateria, corrente_bateria, nivel_bateria,
            valor_ads_hs, tensao_hs, corrente_hs, nivel_agua
        ]:
            print("Feed ignorado por dados inválidos:", feed)
            continue

        cursor.execute("""
            INSERT INTO leituras_iot (
                created_at,
                valor_ads_bateria, tensao_bateria, corrente_bateria, nivel_bateria,
                valor_ads_hs, tensao_hs, corrente_hs, nivel_agua,
                nome_canal, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            created_at,
            valor_ads_bateria, tensao_bateria, corrente_bateria, nivel_bateria,
            valor_ads_hs, tensao_hs, corrente_hs, nivel_agua,
            nome_canal, latitude, longitude
        ))
        conn.commit()

    return {"status": "ok"}
