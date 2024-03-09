import re

import scrapy

from infolibras.items import DefinicaoItem

padrao = re.compile(r"^(.*)(\([^)]*\))$")


class DitechSpider(scrapy.Spider):
    name = "ditech"
    allowed_domains = ["ditech.com.br"]
    start_urls = ["https://www.dictech.com.br/dicionario/termos-tecnicos/informatica/"]

    def parse(self, response):
        for link in response.css("#content li > a::attr(href)").extract():
            yield scrapy.Request(link, callback=self.parse_term, dont_filter=True)

    def parse_term(self, response):
        definicao = DefinicaoItem()
        definicao["variacoes"] = []

        termo = (
            response.css("h1.header-post-title-class::text")
            .extract_first()
            .strip()
            .capitalize()
        )

        correspondencia = padrao.match(termo)
        tem_parenteses = correspondencia is not None

        if correspondencia is not None:
            termo = correspondencia.group(1).strip().capitalize()
            variacao = correspondencia.group(2).strip()[1:-1].capitalize()
            definicao["variacoes"] = [{"variacao": variacao, "explicacao": ""}]

        definicao["termo"] = termo
        definicao_infos = (
            response.css(".entry-content p::text").extract_first().strip().split(": ")
        )

        if len(definicao_infos) > 1:
            definicao["definicao"] = "".join(definicao_infos[1:])
            termo_info = definicao_infos[0].strip().capitalize()

            correspondencia = padrao.match(termo_info)

            if correspondencia is not None:
                termo_info = correspondencia.group(1).strip().capitalize()

                if termo_info != definicao["termo"] and (
                    not tem_parenteses
                    or termo_info != definicao["variacoes"][0]["variacao"]
                ):
                    definicao["variacoes"] = [
                        {"variacao": termo_info, "explicacao": ""}
                    ]

                variacao = correspondencia.group(2).strip()[1:-1].capitalize()

                if variacao != definicao["termo"] and (
                    not tem_parenteses
                    or variacao != definicao["variacoes"][0]["variacao"]
                ):
                    definicao["variacoes"] = [{"variacao": variacao, "explicacao": ""}]

            else:
                if termo_info != definicao["termo"] and (
                    not tem_parenteses
                    or termo_info != definicao["variacoes"][0]["variacao"]
                ):
                    definicao["variacoes"] = [
                        {
                            "variacao": definicao_infos[0].strip().capitalize(),
                            "explicacao": "",
                        }
                    ]

        else:
            definicao["definicao"] = definicao_infos[0].strip()

        definicao["fonte"] = response.url

        yield definicao
