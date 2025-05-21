import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware  # <- CORS para FastAPI
import psycopg2

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

@app.post("/receber-dado")
async def receber_dado(request: Request):
    data = await request.json()
    for feed in data.get("feeds", []):
        cursor.execute("""
            INSERT INTO leituras_iot (
                created_at, valor_ads_bateria, tensao_bateria,
                valor_ads_hs, tensao_hs, corrente_hs, nivel_agua
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            feed['created_at'],
            float(feed['field1']),
            float(feed['field2']),
            float(feed['field5']),
            float(feed['field6']),
            float(feed['field7']),
            float(feed['field8'])
        ))
        conn.commit()

    return {"status": "ok"}
