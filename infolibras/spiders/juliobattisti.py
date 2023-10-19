import scrapy


class JuliobattistiSpider(scrapy.Spider):
    name = "juliobattisti"
    allowed_domains = ["juliobattisti.com.br"]
    start_urls = ["https://www.juliobattisti.com.br/tutoriais/keniareis/dicionarioinfo001.asp"]

    def parse(self, response):
        for termo in response.css("#conteudo_cont p:has(strong)")[2:]:
            yield {
                "termo": termo.css("strong::text").extract_first()[:-2],
                "definição": termo.css("::text").extract()[2].strip()
            }
