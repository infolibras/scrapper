import scrapy


class DefinicaoItem(scrapy.Item):
    termo = scrapy.Field()
    variacoes = scrapy.Field()
    definicao = scrapy.Field()
    fonte = scrapy.Field()
