from scrapy import Request
from scrapy import Spider
from scrapy.selector import Selector
from scrapy import FormRequest
from crawler.items import LaptopTCCItem
# from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
# from selenium.webdriver.chrome.webdriver import WebDriver as ChromeDriver
# from webdriver_manager.chrome import ChromeDriverManager

import datetime
import uuid
import json
aa=1

# <form class="variations_form" action="https://l" method="post" enctype="multipart/form-data" data-product_id="41423" \
#     data-product="[
# {"attributes":{"attribute_cau-hinh":"Core i7-1355U, RAM 16GB, SSD 1TB, FHD [21.900.000]"} },
# {"attributes":{"attribute_cau-hinh":"Core i5-1335U, RAM 8GB, SSD 512GB, FHD [14.900.000]"}}
# ]" current-image="41432">
	

class CrawlerSpider(Spider):
    name = "laptoptcc"
    allowed_domains = ["laptoptcc.com"]
    start_urls = [
        "https://laptoptcc.com/danh-muc/laptop/",
    ]
    def __init__(self):
        options = Options()
        options.headless = True

        self.driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver_linux64',options=options)




    def parse(self, response):
        print(response.url)
        i=1
        links= Selector(response).xpath('//*[@id="main"]/ul[contains(@class, "products")]/li/div/div/div[@class="product-loop-header"]/a/@href')
        for link in links:
            print(i)
            i=i+1
            yield Request(url=link.get(), callback=self.parse_laptop)
        
           
        next_page =  Selector(response).xpath(' //*[@id="main"]/div[@class="shop-control-bar-bottom"]/nav/ul/li/a[@class="next page-numbers"]/@href').extract_first()
        print("*********")
        print(len(links))
        print(next_page)
        print("************")
        if  next_page is not None:
            print(next_page)
            yield Request(next_page, callback=self.parse)
        # yield Request(url="https://laptoptcc.com/cua-hang/laptop-hp-elitebook-840-g5-i5-8350u-8gb-256gb-14-fhd/", callback=self.parse_laptop)


    def parse_laptop(self, response):
        print("**************crawling "+response.url+"***********")

        URL=response.url
        
    
        Name = Selector(response).xpath('//*[@id="content"]/div[@class="container"]/div[@class="site-content-inner"]\
                                        /div[@class="bg-wrapper"]/div[@class="product-detail"]/div[contains(@class,"product")]\
                                        /div/div[@class="col-md-9"]/div/h1[@class="product_title entry-title"]/text()').get()
        
        self.driver.get(response.url)
        element = WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.ID, "cau-hinh"))
        )

        # options = self.driver.find_elements(By.XPATH, '//*[@id="cau-hinh"]/option[contains(@class,"attached")]')
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, '//ul[@class="variable-items-wrapper button-variable-wrapper reselect-clear"]/li')))
        li_tags = self.driver.find_elements(By.XPATH,'//ul[@class="variable-items-wrapper button-variable-wrapper reselect-clear"]/li')

        for i in range(len(li_tags)):
            li = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, f'//ul[@class="variable-items-wrapper button-variable-wrapper reselect-clear"]/li[{i+1}]')))
            li.click()
        
            # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'true')", li)
            wait = WebDriverWait(self.driver, 5)
            # html = self.driver.page_source
            # response = Selector(text=html)
         
            item = LaptopTCCItem()
            item['Name']= Name
            item['URL'] = URL 
            item['Price'] = li.find_element(By.CLASS_NAME,"price-mini").text
           
            item['Ram'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "Ram")]').text
            item['Storage'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "Ổ cứng")]').text
            item['Display'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "Màn hình")]').text
            item['Graphics'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "VGA")]').text
            item['Status'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "Tình trạng")]').text
            item['CPU'] = self.driver.find_element(By.XPATH,'/html/body/div[2]/div/div[4]/div/div/div/div[2]/div[1]/div/div[1]/div/div[2]/form/div[1]/div/div[2]/p/span[contains(b/text(), "CPU")]').text
            # self.driver.execute_script("arguments[0].setAttribute('aria-checked', 'false')", li)
            yield item


    def closed(self, reason):
        # Close the browser when the spider is done
        self.driver.quit()       


   