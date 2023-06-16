from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from crawler.items import Laptop88Item
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import datetime
import uuid
aa=1

class CrawlerSpider(Spider):
    name = "laptop88"
    allowed_domains = ["laptop88.vn"]
    start_urls = [
        "https://laptop88.vn/laptop-cu.html",
        "https://laptop88.vn/laptop-moi.html    "
    ]
    
    def parse(self, response):
           
        pages = Selector(response).xpath('/html/body/main/div[contains(@class,"product-category")]/div[contains(@class,"container")]/div[contains(@class,"paging")]/a/@href').extract()
        for page in pages:
    
            link_page ="https://laptop88.vn" + page
            yield Request(link_page, callback=self.parse_link)
        # yield Request(url="https://laptop88.vn/laptop-cu-hp-probook-440-g5-intel-core-i5-8250u-14-inch-full-hd.html", callback=self.parse_laptop)
        
    def parse_link(self,response):
        print(response.url)

        
        # links= Selector(response).xpath('/html/body/main/div[contains(@class,"product-category")]/div[contains(@class,"container")]/div[contains(@class,"product-list")]/div[contains(@class,"product-item")]/div[contains(@class,"product-info")]/div[contains(@class,"product-title")]/a/@href').extract()
        items= Selector(response).xpath('/html/body/main/div[contains(@class,"product-category")]/div[contains(@class,"container")]/div[contains(@class,"product-list")]/div[contains(@class,"product-item")]')
        for item in items:

            URL = item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-title")]/a/@href').get()
        
            ImgURL = item.xpath('.//div[contains(@class,"product-img")]/a/img/@src').get()
            URL = "https://laptop88.vn" + URL
            ImgURL = "https://laptop88.vn" + ImgURL
            Name = item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-title")]/a/text()').get()
            Price = item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-price")]/div[contains(@class,"price-bottom")]/span[contains(@class,"item-price")]/text()').get()

            CPU=item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-promotion")]/table/tbody/tr[1]/td[2]').xpath('string()').extract()
            Ram=item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-promotion")]/table/tbody/tr[2]/td[2]').xpath('string()').extract()
            Storage=item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-promotion")]/table/tbody/tr[3]/td[2]').xpath('string()').extract()
            Display=item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-promotion")]/table/tbody/tr[5]/td[2]').xpath('string()').extract()
            Graphics=item.xpath('.//div[contains(@class,"product-info")]/div[contains(@class,"product-promotion")]/table/tbody/tr[4]/td[2]').xpath('string()').extract()   

            yield Request(URL, callback=self.parse_status,meta={'Name': Name, 'Price': Price, 'URL': URL,'CPU': CPU,'Ram': Ram,'Storage': Storage,'Display': Display,'Graphics': Graphics,'ImgURL': ImgURL })
    def parse_status (self, response):
        
        print(response.url)
        status = Selector(response).xpath('/html/body/main/div[2]/div/div[1]/div[2]/div[3]/div/a[1]/text()').get()

        item = Laptop88Item()
        item['URL'] = response.url
        item['Name'] = response.meta['Name']
        item['Price'] = response.meta['Price']
        item['CPU'] = response.meta['CPU']
        item['Ram'] = response.meta['Ram']
        item['Storage'] =  response.meta['Storage']
        item['Display'] = response.meta['Display']
        item['Graphics'] = response.meta['Graphics']
        item['Status'] = status
        item['ImgURL'] =response.meta['ImgURL']
        yield item


 

        # # ImgURL = response.xpath('//*[@id="noibat"]/div[contains(@class,"owl-stage-outer")]/div[contains(@class,"owl-stage")]/div[contains(@class,"owl-item")]/div[contains(@class,"item_image")]/a[contains(@class,"show_popup")]/img/@src').get()
        # ImgURL = response.xpath('//*[@id="noibat"]/div[1]/div/div[1]/div/a/img/@src').get()
        # count = Selector(response).xpath('//div[@class="list_same"]/count(a)').extract_first()

    
     

        

            
