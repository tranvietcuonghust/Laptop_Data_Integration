from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from crawler.items import PhucAnhItem
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
    name = "phucanh"
    allowed_domains = ["phucanh.vn"]
    start_urls = [
        "https://www.phucanh.vn/laptop.html",
    ]
    
    def parse(self, response):
        print(response.url)
        i=1
        

        links= Selector(response).xpath('/html/body/div[2]/div[@class="product-list-group"]/ul/li/div[@class="p-container"]/a[1]/@href')
        products= Selector(response).xpath('/html/body/div[2]/div[@class="product-list-group"]/ul/li/div[@class="p-container"]')
        for product in products:
            print(i)
            i=i+1
            link = product.xpath('./a[1]/@href').get()
            ImgURL = product.xpath('./a[1]/img/@data-src').get()
            link = "https://www.phucanh.vn/" + link
            yield Request(url=link, callback=self.parse_laptop,cb_kwargs=dict(ImgURL=ImgURL))
        


          
        next_page =  Selector(response).xpath('/html/body/div[2]/div[@class="paging"]/a[contains(text(), "Tiếp theo")]/@href').extract_first()
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if  next_page is not None:
            next_page = "https://www.phucanh.vn/"+ next_page
            print(next_page)
            yield Request(next_page, callback=self.parse)
        # yield Request(url="https://trungtran.vn/lenovo-thinkpad-x1-carbon-gen7-i5-nhieu-cau-hinh/?product=,542", callback=self.parse_laptop)


    def parse_laptop(self, response,ImgURL):
        print("**************crawling "+response.url+"***********")
        # print(response.url)
        # HOST="https://tranvietcuonghust.documents.azure.com:443/"
        # MASTER_KEY="Bx8F7H61NwRqoXyjPBeqaejtzXO3gZXWePCEqpOI4awtCg1KqosrmnhlRNC687WxGHSodF2f9VFWACDbl6jGxg=="

        # item['Price'] = response.xpath('//*[@id="main_container"]/section/aside[2]/div[1]/div[1]/p/span[1]/text()').extract_first()
        # print(response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[16]/td[2]/text()').get())

        # print(response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[2]/td[2]/text()').get())
        # CPU=response.xpath('//*[@id="charactestic_detail"]/div[2]/div/div[2]/table/tr[2]/td[2]/text()').get()
         
        URL      = response.url
        
        
        Name     = response.xpath('/html/body/div[2]/h1/text()').get()
        
        
        Price    = response.xpath('//*[@id="product-info-price"]/table/tr[1]/td[2]/span/text()').get()
        
        CPU      = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Công nghệ CPU")]/td[@class="spec-value"]').xpath('string()').extract()
        CPU_code = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Mã CPU")]/td[@class="spec-value"]').xpath('string()').extract()
        CPU_speed= response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Tốc độ CPU")]/td[@class="spec-value"]').xpath('string()').extract()
        Num_cores= response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Số lõi CPU")]/td[@class="spec-value"]').xpath('string()').extract()
        Num_thread=response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Số luồng")]/td[@class="spec-value"]').xpath('string()').extract()
        Ram      = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Dung lượng RAM")]/td[@class="spec-value"]').xpath('string()').extract()
        Ram_type = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Loại RAM")]/td[@class="spec-value"]').xpath('string()').extract()
        Ram_speed= response.xpath('//*[@id="fancybox-spec"]/div[1]/table//tr[contains(td[@class="spec-key"]/text(), "Tốc độ Bus RAM")]/td[@class="spec-value"]').xpath('string()').extract()
        Ram_slot = response.xpath('//*[@id="fancybox-spec"]/div[1]/table//tr[contains(td[@class="spec-key"]/text(), "Khe cắm RAM")]/td[@class="spec-value"]').xpath('string()').extract()
        Storage  = response.xpath('//*[@id="fancybox-spec"]/div[1]/table//tr[contains(td[@class="spec-key"]/text(), "Dung lượng ổ cứng")]/td[@class="spec-value"]').xpath('string()').extract()
        Storage_type  = response.xpath('//*[@id="fancybox-spec"]/div[1]/table//tr[contains(td[@class="spec-key"]/text(), "Loại ổ cứng")]/td[@class="spec-value"]').xpath('string()').extract()
        Storage_drive = response.xpath('//*[@id="fancybox-spec"]/div[1]/table//tr[contains(td[@class="spec-key"]/text(), "Chuẩn giao tiếp ổ cứng")]/td[@class="spec-value"]').xpath('string()').extract()
        Graphics = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Card đồ họa")]/td[@class="spec-value"]').xpath('string()').extract()
        Display  = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Kích thước màn hình")]/td[@class="spec-value"]').xpath('string()').extract()
        Resolutions = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Độ phân giải")]/td[@class="spec-value"]').xpath('string()').extract()
        Wireless    = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Kết nối không dây")]/td[@class="spec-value"]').xpath('string()').extract()
        Ports    = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Cổng giao tiếp")]/td[@class="spec-value"]').xpath('string()').extract()
        OS       = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Hệ điều hành")]/td[@class="spec-value"]').xpath('string()').extract()
        Battery  = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Thông số pin")]/td[@class="spec-value"]').xpath('string()').extract()
        Size     = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Kích thước")]/td[@class="spec-value"]').xpath('string()').extract()
        Weight   = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Trọng lượng")]/td[@class="spec-value"]').xpath('string()').extract()
        Color    = response.xpath('//*[@id="fancybox-spec"]/div[1]/table/tr[contains(td[@class="spec-key"]/text(), "Màu sắc")]/td[@class="spec-value"]').xpath('string()').extract()
       
        item = PhucAnhItem()
        item['URL'] = URL
        item['Name'] = Name
        item['Price'] = Price
        item['CPU'] = CPU
        item['Ram'] = Ram
        item['Storage'] =  Storage
        item['Display'] = Display
        item['Graphics'] = Graphics
        item['Battery'] = Battery
        item['OS'] = OS
        item['Wireless'] = Wireless
        item['Size'] = Size
        item['Weight'] =  Weight
        item['Color'] = Color

        item['CPU_code'] = CPU_code
        item['CPU_speed'] = CPU_speed
        item['Num_cores'] = Num_cores
        item['Num_thread'] = Num_thread
        item['Ram_type'] = Ram_type
        item['Ram_speed'] = Ram_speed
        item['Ram_slot'] = Ram_slot
        item['Storage_type'] = Storage_type
        item['Storage_drive'] = Storage_drive
        item['Resolutions'] = Resolutions
        item['Ports'] = Ports
        item['ImgURL'] = ImgURL

        yield item
            

            
