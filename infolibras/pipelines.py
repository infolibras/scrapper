import os

import mysql.connector
import typesense
from dotenv import load_dotenv
from slugify import slugify


class InfolibrasPipeline:
    def __init__(self):
        load_dotenv()

        self.conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_DATABASE")
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
                    { "name": "id", "type": "string" },
                    { "name": "termo", "type": "string" },
                    { "name": "variacoes", "type": "string[]" }
                ]
            })

    def process_item(self, item, spider):
        item.setdefault("variacoes", [])

        self.cur.execute(
            """SELECT termo.id AS id
                FROM variacao
                LEFT JOIN termo
                    ON variacao.idTermo = termo.id
                WHERE variacao.variacao IN (%s)""",
            (",".join([item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]]),)
        )

        termo = self.cur.fetchone()

        if termo is None:
            self.cur.execute("INSERT INTO termo (termo, slug) VALUES (%s, %s)", (item["termo"], slugify(item["termo"])))
            termo_id = self.cur.lastrowid
            self.cur.execute("INSERT INTO variacao (idTermo, variacao, slug) VALUES (%s, %s, %s)", (termo_id, item["termo"], slugify(item["termo"])))
        else:
            termo_id = termo[0]

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
                "variacoes": list(set(document["variacoes"] + [item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]]))
            })

        except typesense.exceptions.ObjectNotFound:
            self.client.collections["gooli-termos"].documents.create({
                "id": str(termo_id),
                "termo": item["termo"],
                "variacoes": [item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]]
            })

        self.conn.commit()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
