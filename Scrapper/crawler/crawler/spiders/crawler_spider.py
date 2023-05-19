from scrapy import Spider
from scrapy.selector import Selector
from crawler.items import CrawlerItem

class CrawlerSpider(Spider):
    name = "crawler"
    allowed_domains = ["thegioididong.com"]
    start_urls = [
        "https://www.thegioididong.com/dtdd/samsung-galaxy-a50",
    ]
    def parse(self, response):
        questions = Selector(response).xpath('//*[@id="comment-list0"]/ul[@class="comment-list"]/li')

        for question in questions:
            item = CrawlerItem()
            item['User'] = question.xpath(
                'div[@class="cmt-top"]/p[@class="cmt-top-name"]/text()').extract_first()
            item['Comment'] = question.xpath(
                'div[@class="cmt-content"]/p/text()').extract_first()
            item['Time'] = question.xpath(
                'div[@class="cmt-command"]/span/text()').extract_first()

            yield item
