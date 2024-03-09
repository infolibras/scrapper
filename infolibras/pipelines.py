import os
from typing import Optional

import mysql.connector
import numpy as np
import typesense
from dotenv import dotenv_values
from numpy.linalg import norm
from openai import OpenAI
from pypika import MySQLQuery as Query
from pypika import Table
from slugify import slugify


class TypedTable(Table):
    __table__ = ""

    def __init__(
        self,
        name: Optional[str] = None,
        schema: Optional[str] = None,
        alias: Optional[str] = None,
        query_cls: Optional[Query] = None,
    ) -> None:
        if name is None:
            if self.__table__:
                name = self.__table__
            else:
                name = self.__class__.__name__

        super().__init__(name, schema, alias, query_cls)


class Termo(TypedTable):
    __table__ = "termo"

    id: int
    termo: str
    slug: str


class Variacao(TypedTable):
    __table__ = "variacao"

    id: int
    idTermo: int
    variacao: str
    slug: str


class Definicao(TypedTable):
    __table__ = "definicao"

    id: int
    idTermo: int
    definicao: str
    fonte: str


termo_tb = Termo()
variacao_tb = Variacao()
definicao_tb = Definicao()


def cosine_similarity(a: list[float], b: list[float]) -> float:
    return np.dot(a, b) / (norm(a) * norm(b))


class InfolibrasPipeline:
    def __init__(self):
        env = dotenv_values()

        self.openai = OpenAI(api_key=env["OPENAI_API_KEY"])

        self.conn = mysql.connector.connect(
            host=env["DB_HOST"],
            port=int(env["DB_PORT"]),
            user=env["DB_USER"],
            password=env["DB_PASSWORD"],
            database=env["DB_NAME"],
        )

        self.cur = self.conn.cursor()

        self.client = typesense.Client(
            {
                "nodes": [
                    {
                        "host": env["TYPESENSE_HOST"],
                        "port": int(env["TYPESENSE_PORT"]),
                        "protocol": env["TYPESENSE_PROTOCOL"],
                    }
                ],
                "api_key": env["TYPESENSE_API_KEY"],
                "connection_timeout_seconds": 2,
            }
        )

        if "gooli-termos" not in tuple(
            map(
                lambda collection: collection["name"],
                self.client.collections.retrieve(),
            )
        ):
            self.client.collections.create(
                {
                    "name": "gooli-termos",
                    "fields": [
                        {"name": "termo", "type": "string"},
                        {"name": "variacoes", "type": "string[]"},
                        {"name": "definicoes", "type": "string[]"},
                        {"name": "quantidade_definicoes", "type": "int32"},
                        {
                            "name": "contem_video",
                            "type": "bool",
                            "optional": True,
                            "index": False,
                        },
                        {
                            "name": "categorias",
                            "type": "string[]",
                            "optional": True,
                            "facet": True,
                        },
                        {"name": "slug", "type": "string"},
                        {
                            "name": "embedding",
                            "type": "float[]",
                            "embed": {
                                "from": ["termo", "variacoes", "definicoes"],
                                "model_config": {
                                    "model_name": "openai/text-embedding-3-large",
                                    "api_key": os.getenv("OPENAI_API_KEY"),
                                },
                            },
                        },
                    ],
                }
            )

            self.client.collections.create(
                {
                    "name": "gooli-termos-queries",
                    "fields": [
                        {"name": "q", "type": "string"},
                        {"name": "count", "type": "int32"},
                    ],
                }
            )

            self.client.analytics.rules.upsert(
                "gooli-termos-queries-aggregation",
                {
                    "type": "popular_queries",
                    "params": {
                        "source": {"collections": ["gooli-termos"]},
                        "destination": {"collection": "gooli-termos-queries"},
                        "limit": 1000,
                    },
                },
            )

    def process_item(self, item, spider):
        item.setdefault("variacoes", [])

        self.cur.execute(
            Query.from_(termo_tb)
            .select("id")
            .where(termo_tb.termo == item["termo"])
            .get_sql()
        )

        termo = self.cur.fetchone()

        if termo is None:
            self.cur.execute(
                Query.into(termo_tb)
                .columns(termo_tb.termo, termo_tb.slug)
                .insert(item["termo"], slugify(item["termo"]))
                .get_sql()
            )
            termo_id = self.cur.lastrowid
            self.cur.execute(
                Query.into(variacao_tb)
                .columns(variacao_tb.idTermo, variacao_tb.variacao, variacao_tb.slug)
                .insert(termo_id, item["termo"], slugify(item["termo"]))
                .get_sql()
            )
        else:
            termo_id = termo[0]

        self.cur.execute(
            Query.from_(definicao_tb)
            .select(termo_tb.id)
            .where(
                (definicao_tb.idTermo == termo_id) & (definicao_tb.definicao)
                == item["definicao"]
            )
            .get_sql()
        )

        definicao = self.cur.fetchone()

        if definicao is None:
            self.cur.execute(
                Query.into(definicao_tb)
                .columns(
                    definicao_tb.idTermo, definicao_tb.definicao, definicao_tb.fonte
                )
                .insert(termo_id, item["definicao"], item["fonte"])
                .get_sql()
            )

        for variacao in item["variacoes"]:
            self.cur.execute(
                Query.from_(variacao_tb)
                .select(variacao_tb.id)
                .where(
                    (variacao_tb.idTermo == termo_id) & (variacao_tb.variacao)
                    == variacao["variacao"]
                )
                .get_sql()
            )
            variacao_id = self.cur.fetchone()

            if variacao_id is None:
                self.cur.execute(
                    Query.into(variacao_tb)
                    .columns(
                        variacao_tb.idTermo, variacao_tb.variacao, variacao_tb.slug
                    )
                    .insert(
                        termo_id, variacao["variacao"], slugify(variacao["variacao"])
                    )
                    .get_sql()
                )
                variacao_id = self.cur.lastrowid

        try:
            document = (
                self.client.collections["gooli-termos"].documents[termo_id].retrieve()
            )

            self.client.collections["gooli-termos"].documents[str(termo_id)].update(
                {
                    "variacoes": list(
                        set(
                            document["variacoes"]
                            + [item["termo"]]
                            + [variacao["variacao"] for variacao in item["variacoes"]]
                        )
                    ),
                    "quantidade_definicoes": document["quantidade_definicoes"] + 1,
                    "definicoes": list(
                        set(document["definicoes"] + [item["definicao"]])
                    ),
                }
            )

        except typesense.exceptions.ObjectNotFound:
            self.client.collections["gooli-termos"].documents.create(
                {
                    "id": str(termo_id),
                    "termo": item["termo"],
                    "variacoes": [item["termo"]]
                    + [variacao["variacao"] for variacao in item["variacoes"]],
                    "definicoes": [item["definicao"]],
                    "quantidade_definicoes": 1,
                    "contem_video": False,
                    "slug": slugify(item["termo"]),
                }
            )

        self.conn.commit()

        return item

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()
