import scrapy
import re

from infolibras.items import DefinicaoItem


class DouglasgasparSpider(scrapy.Spider):
    name = "douglasgaspar"
    allowed_domains = ["douglasgaspar.wordpress.com"]
    start_urls = ["https://douglasgaspar.wordpress.com/2020/03/17/glossario-com-termos-de-ti-informatica-e-programacao/"]

    def parse(self, response):
        for termo in response.css(".content li"):
            text = termo.css("::text").extract()
            variacao = termo.css("em::text").extract_first()

            if len(text) > 0:
                definicao = DefinicaoItem()
                definicao["variacoes"] = [{ "variacao": variacao, "explicacao": "" }] if variacao is not None else []
                definicao["fonte"] = "https://douglasgaspar.wordpress.com/2020/03/17/glossario-com-termos-de-ti-informatica-e-programacao/"

                if len(text) == 1:
                    text = text[0].split(" – ")
                    definicao["termo"] = re.sub(r'\([^)]*\)', '', text[0]).strip().capitalize()
                    definicao["definicao"] = "".join(text[1:]).strip().capitalize()

                else:
                    definicao["termo"] = re.sub(r'\([^)]*\)', '', text[0][:-2]).strip().capitalize()
                    definicao["definicao"] = text[2][4:].strip().capitalize()

                if definicao["definicao"] and definicao["termo"]:
                    yield definicao
