import os

import mysql.connector
import typesense
from dotenv import load_dotenv
from slugify import slugify
from openai import OpenAI
from numpy.linalg import norm
import numpy as np

def cosine_similarity(a: list[float], b: list[float]) -> float:
    return np.dot(a, b) / (norm(a) * norm(b))


class InfolibrasPipeline:
    def __init__(self):
        load_dotenv()

        self.openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        self.conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )

        self.cur = self.conn.cursor()

        self.client = typesense.Client({
            "nodes": [{
                "host": os.getenv("TYPESENSE_HOST"),
                "port": int(os.getenv("TYPESENSE_PORT")),
                "protocol": os.getenv("TYPESENSE_PROTOCOL")
            }],
            "api_key": os.getenv("TYPESENSE_API_KEY"),
            "connection_timeout_seconds": 2
        })

        if "gooli-termos" not in [*map(lambda collection: collection["name"], self.client.collections.retrieve())]:
            self.client.collections.create({
                "name": "gooli-termos",
                "fields": [
                    { "name": "termo", "type": "string" },
                    { "name": "variacoes", "type": "string[]" },
                    { "name": "definicoes", "type": "string[]" },
                    { "name": "quantidade_definicoes", "type": "int32" },
                    { "name": "contem_video", "type": "bool", "optional": True, "index": False },
                    { "name": "categorias", "type": "string[]", "optional": True, "facet": True },
                    { "name": "slug", "type": "string" },
                    {
                        "name": "embedding",
                        "type": "float[]",
                        "embed": {
                            "from": ["termo", "variacoes", "definicoes"],
                            "model_config": {
                                "model_name": "openai/text-embedding-ada-002",
                                "api_key": os.getenv("OPENAI_API_KEY")
                            }
                        }
                    }
                ]
            })

            self.client.collections.create({
                "name": "gooli-termos-queries",
                "fields": [
                    {"name": "q", "type": "string" },
                    {"name": "count", "type": "int32" }
                ]
            })

            self.client.analytics.rules.upsert("gooli-termos-queries-aggregation", {
                "type": "popular_queries",
                "params": {
                    "source": {
                        "collections": ["gooli-termos"]
                    },
                    "destination": {
                        "collection": "gooli-termos-queries"
                    },
                    "limit": 1000
                }
            })

    def process_item(self, item, spider):
        item.setdefault("variacoes", [])

        self.cur.execute(
            """SELECT termo.id AS id
                FROM termo
                WHERE termo = %s""",
            (item["termo"],)
        )

        termo = self.cur.fetchone()

        if termo is None:
            self.cur.execute("INSERT INTO termo (termo, slug) VALUES (%s, %s)", (item["termo"], slugify(item["termo"])))
            termo_id = self.cur.lastrowid
            self.cur.execute("INSERT INTO variacao (idTermo, variacao, slug) VALUES (%s, %s, %s)", (termo_id, item["termo"], slugify(item["termo"])))
        else:
            termo_id = termo[0]

        self.cur.execute(
            """SELECT definicao.id AS id
                FROM definicao
                WHERE idTermo = %s AND definicao = %s""",
            (termo_id, item["definicao"])
        )

        definicao = self.cur.fetchone()

        if definicao is None:
            self.cur.execute("INSERT INTO definicao (idTermo, definicao, fonte) VALUES (%s, %s, %s)", (termo_id, item["definicao"], item["fonte"]))

        for variacao in item["variacoes"]:
            self.cur.execute("SELECT id FROM variacao WHERE idTermo = %s AND variacao = %s", (termo_id, variacao["variacao"]))
            variacao_id = self.cur.fetchone()

            if variacao_id is None:
                self.cur.execute("INSERT INTO variacao (idTermo, variacao, slug) VALUES (%s, %s, %s)", (termo_id, variacao["variacao"], slugify(variacao["variacao"])))
                variacao_id = self.cur.lastrowid

        try:
            document = self.client.collections["gooli-termos"].documents[termo_id].retrieve()

            self.client.collections["gooli-termos"].documents[str(termo_id)].update({
                "variacoes": list(set(document["variacoes"] + [item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]])),
                "quantidade_definicoes": document["quantidade_definicoes"] + 1,
                "definicoes": list(set(document["definicoes"] + [item["definicao"]]))
            })

        except typesense.exceptions.ObjectNotFound:
            self.client.collections["gooli-termos"].documents.create({
                "id": str(termo_id),
                "termo": item["termo"],
                "variacoes": [item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]],
                "definicoes": [item["definicao"]],
                "quantidade_definicoes": 1,
                "contem_video": False,
                "slug": slugify(item["termo"])
            })

        self.conn.commit()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
