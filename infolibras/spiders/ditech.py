import scrapy


class DitechSpider(scrapy.Spider):
    name = "ditech"
    allowed_domains = ["ditech.com.br"]
    start_urls = ["https://www.dictech.com.br/dicionario/termos-tecnicos/informatica/"]

    def parse(self, response):
        for link in response.css("#content li > a::attr(href)").extract():
            yield scrapy.Request(link, callback=self.parse_term, dont_filter=True)

    def parse_term(self, response):
        yield {
            "termo": response.css("h1.header-post-title-class::text").extract_first(),
            "definição": response.css(".entry-content p::text").extract_first()
        }
