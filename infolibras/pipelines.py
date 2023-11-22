import os

import mysql.connector
from dotenv import load_dotenv
from itemadapter import ItemAdapter


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

    def process_item(self, item, spider):
        item.setdefault("variacoes", [])

        self.cur.execute("SELECT id FROM Termo WHERE termo in (%s)" % ", ".join(["%s"] * (len(item["variacoes"]) + 1)), [item["termo"]] + [variacao["variacao"] for variacao in item["variacoes"]])
        termo = self.cur.fetchone()

        if termo is None:
            self.cur.execute("INSERT INTO Termo (termo) VALUES (%s)", (item["termo"],))
            termo_id = self.cur.lastrowid
        else:
            termo_id = termo[0]

        self.cur.execute("INSERT INTO Definicao (idTermo, definicao, fonte) VALUES (%s, %s, %s)", (termo_id, item["definicao"], item["fonte"]))

        for variacao in item["variacoes"]:
            self.cur.execute("SELECT id FROM Variacao WHERE idTermo = %s AND variacao = %s", (termo_id, variacao["variacao"]))
            variacao_id = self.cur.fetchone()

            if variacao_id is None:
                self.cur.execute("INSERT INTO Variacao (idTermo, variacao, explicacao) VALUES (%s, %s, %s)", (termo_id, variacao["variacao"], variacao["explicacao"]))
                variacao_id = self.cur.lastrowid

        self.conn.commit()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
