from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from crawler.items import TrungTranItem
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
    name = "trungtran"
    allowed_domains = ["trungtran.vn"]
    start_urls = [
        "https://trungtran.vn/laptop/",
    ]
    
    def parse(self, response):
        print(response.url)
        i=1
        products= Selector(response).xpath('//*[@id="box_product"]/div[contains(@class,"col-sm-5ths")]')
        for product in products:
            print(i)
            i=i+1
            link = product.xpath('.//div/div/a[2]/@href')
            ImgURL= product.xpath('.//div/div/a[1]/div/picture/img/@data-src').get()
            yield Request(url=link.get(), callback=self.parse_laptop,cb_kwargs=dict(ImgURL=ImgURL))
        

        next_page =  Selector(response).xpath(' //*[@id="main_container"]/div[contains(@class,"product_cat")]/div[contains(@class,"col-md-9")]/div[contains(@class,"pagination")]/a[contains(@class,"next-page")]/@href').extract_first()
        print("*********")
        print(len(products))
        print(next_page)
        print("************")
        if  next_page is not None:
            next_page = "https://trungtran.vn"+ next_page
            print(next_page)
            yield Request(next_page, callback=self.parse)s
        # yield Request(url="https://trungtran.vn/lenovo-thinkpad-x1-carbon-gen7-i5-nhieu-cau-hinh/?product=,542", callback=self.parse_laptop)


    def parse_laptop(self, response, ImgURL):
        print("**************crawling "+response.url+"***********")
        # print(response.url)
        # HOST="https://tranvietcuonghust.documents.azure.com:443/"
        # MASTER_KEY="Bx8F7H61NwRqoXyjPBeqaejtzXO3gZXWePCEqpOI4awtCg1KqosrmnhlRNC687WxGHSodF2f9VFWACDbl6jGxg=="

        # item['Price'] = response.xpath('//*[@id="main_container"]/section/aside[2]/div[1]/div[1]/p/span[1]/text()').extract_first()
        # print(response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[16]/td[2]/text()').get())

        # print(response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[2]/td[2]/text()').get())
        # CPU=response.xpath('//*[@id="charactestic_detail"]/div[2]/div/div[2]/table/tr[2]/td[2]/text()').get()
        URL      = response.url
        Name     = response.xpath('/html/body/div[1]/div[6]/div/div[3]/section/div[1]/div/div[2]/div[1]/div[1]/h2[1]/strong/text()').get()
        Price    = response.xpath('//*[@id="main_container"]/section/aside[2]/div[1]/div[1]/p/span[1]/text()').get()
        CPU      = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[1]/td[contains(@class,"content_charactestic")]/text()').get()
        Ram      = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[2]/td[contains(@class,"content_charactestic")]/text()').get()
        Storage  = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[3]/td[contains(@class,"content_charactestic")]/text()').get()
        Display  = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[4]/td[contains(@class,"content_charactestic")]/text()').get()
        Graphics = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[5]/td[contains(@class,"content_charactestic")]/text()').get()
        Battery  = response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[6]/td[contains(@class,"content_charactestic")]/text()').get()
        OS       = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Hệ điều hành")]/td[2]/text()').get()
        Wireless = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Kết nối không dây")]/td[2]/text()').get()
        Size     = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Kích thước máy")]/td[2]/text()').get()
        Weight   = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Trọng lượng")]/td[2]/text()').get()
        Color    = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Màu sắc")]/td[2]/text()').get()
        MFG_year = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Năm sản xuất")]/td[2]/text()').get()
        Brand    = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Hãng sản xuất")]/td[2]/text()').get()
        Status   = response.xpath('//*[@id="charactestic_detail"]/div[contains(@class,"modal-dialog")]/div/div[2]/table/tr[contains(td/text(), "Tình trạng sản phẩm")]/td[2]/text()').get()
        # # ImgURL = response.xpath('//*[@id="noibat"]/div[contains(@class,"owl-stage-outer")]/div[contains(@class,"owl-stage")]/div[contains(@class,"owl-item")]/div[contains(@class,"item_image")]/a[contains(@class,"show_popup")]/img/@src').get()
        # ImgURL = response.xpath('//*[@id="noibat"]/div[1]/div/div[1]/div/a/img/@src').get()
        # count = Selector(response).xpath('//div[@class="list_same"]/count(a)').extract_first()
    
        # a_list_same= Selector(response).xpath('//*[@id="main_container"]/section/aside[2]/div[1]/div[1]/div[1]/div[1]/a[1]')
        a_list_same= Selector(response).xpath('//*[@id="main_container"]/section/aside[contains(@class,"_extra")]/div[contains(@class,"details_top1")]/div[contains(@class,"details_top")]/div[contains(@class,"edit-price")]/div[contains(@class,"list_same")]/a[contains(@class,"item_same")]')
        print(a_list_same)
        if len(a_list_same) != 0:
            for a in a_list_same:
                item = TrungTranItem()
                item['URL'] = response.url
                item['Name'] = a.xpath('.//div[contains(@class,"title_sp")]/p[contains(@class,"nick_name")]/text()').get()
                item['Price'] = a.xpath('.//div[contains(@class,"price_same")]/span[contains(@class,"price_same_2")]/text()').get()
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
                item['MFG_year'] = MFG_year
                item['Brand'] = Brand
                item['Status'] =Status
                item['ImgURL'] = ImgURL
                # aa=aa+1
                # print("#################")
                # print(aa)
                # print("##################")

                yield item
        else:
            item = TrungTranItem()
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
            item['MFG_year'] = MFG_year
            item['Brand'] = Brand
            item['Status'] = Status
            item['ImgURL'] = ImgURL

            yield item

        # item={
        #     "id": str(uuid.uuid4()),
        #     'URL': response.url,
        #     'Name' : response.xpath('//*[@id="picture"]/div/div[1]/h1/text()').extract_first(),
        #     'Price': response.xpath('//*[@id="main_container"]/section/aside[2]/div[1]/div[1]/p/span[1]/text()').extract_first(),
        #     'CPU': response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[1]/td[contains(@class,"content_charactestic")]/text()').get(),
        #     'Storage' : response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[3]/td[contains(@class,"content_charactestic")]/text()').get(),
        #     'Display' : response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[4]/td[contains(@class,"content_charactestic")]/text()').get(),
        #     'Graphics': response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[5]/td[contains(@class,"content_charactestic")]/text()').get(),
        #     'Battery' : response.xpath('//*[@id="thongso"]/div[contains(@class,"_characteristic")]/table[contains(@class,"charactestic_table")]/tr[6]/td[contains(@class,"content_charactestic")]/text()').get(),
        #     'ImgURL'  : response.xpath('//*[@id="noibat"]/div[1]/div/div[1]/div/a/img/@src').get()
        # }

        # client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY}, user_agent="CosmosDBPythonQuickstart", user_agent_overwrite=True)
        # database_name = 'test_Database1'
        # database_id = 'test_Database1'
        # container_id = 'test_Container1'
        # try:
        #     database = client.create_database(id=database_id)
        #     print('Database with id \'{0}\' created'.format(database_id))

        # except exceptions.CosmosResourceExistsError:
        #     database = client.get_database_client(database_name)


        # try:
        #     container = database.create_container(id=container_id, partition_key=PartitionKey(path='/Name'))
        #     print('Container with id \'{0}\' created'.format(container_id))

        # except exceptions.CosmosResourceExistsError:
        #     container = database.get_container_client(container_id)
        #     print('Container with id \'{0}\' was found'.format(container_id))
        # container.create_item(body=item)
            

            
