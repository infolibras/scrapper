import re

import scrapy

from infolibras.items import DefinicaoItem

padrao = re.compile(r"^(.*)(\([^)]*\))$")


class JuliobattistiSpider(scrapy.Spider):
    name = "juliobattisti"
    allowed_domains = ["juliobattisti.com.br"]
    start_urls = ["https://www.juliobattisti.com.br/tutoriais/keniareis/dicionarioinfo001.asp"]

    def parse(self, response):
        for termo in response.css("#conteudo_cont p:has(strong)")[2:]:
            definicao = DefinicaoItem()
            definicao["variacoes"] = []

            termo_texto = termo.css("strong::text").extract_first()[:-2].strip().capitalize()
            correspondencia = padrao.match(termo_texto)

            if correspondencia is not None:
                definicao["termo"] = correspondencia.group(1).strip().capitalize()
                definicao["variacoes"].append({ "variacao": correspondencia.group(2).strip()[1:-1].strip().capitalize(), "explicacao": "" })
            else:
                definicao["termo"] = termo_texto

            definicao_texto = termo.css("::text").extract()[2].strip().capitalize()
            correspondencia = padrao.match(definicao_texto)

            if correspondencia is not None:
                definicao["definicao"] = correspondencia.group(1).strip().capitalize()
                definicao["variacoes"].append({ "variacao": correspondencia.group(2).strip()[1:-1].strip().capitalize(), "explicacao": "" })
            else:
                definicao["definicao"] = definicao_texto

            definicao["fonte"] = "https://www.juliobattisti.com.br/tutoriais/keniareis/dicionarioinfo001.asp"

            yield definicao
