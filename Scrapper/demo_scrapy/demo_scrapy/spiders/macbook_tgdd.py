import scrapy


class MacbookTgddSpider(scrapy.Spider):
    name = "macbook_tgdd"
    allowed_domains = ["www.thegioididong.com"]
    start_urls = ["https://www.thegioididong.com/laptop-apple-macbook"]

    def parse(self, response):
        pass
