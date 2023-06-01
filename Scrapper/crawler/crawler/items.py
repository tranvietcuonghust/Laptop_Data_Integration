import scrapy

# class CrawlerItem(scrapy.Item):
#     # define the fields for your item here like:
    
#     User = scrapy.Field()
#     Comment = scrapy.Field()
#     Time = scrapy.Field()


class TrungTranItem(scrapy.Item):
    URL = scrapy.Field()
    Name = scrapy.Field()
    Price = scrapy.Field()
    CPU = scrapy.Field()
    Storage = scrapy.Field()
    Ram = scrapy.Field()
    Display = scrapy.Field()
    Graphics = scrapy.Field()
    OS = scrapy.Field()
    Wireless = scrapy.Field()
    Size = scrapy.Field()
    Weight = scrapy.Field()
    Color = scrapy.Field()
    MFG_year = scrapy.Field()
    Brand= scrapy.Field()
    Battery = scrapy.Field()
    
class Laptop88Item(scrapy.Item):
    URL = scrapy.Field()
    Name = scrapy.Field()
    Price = scrapy.Field()
    CPU = scrapy.Field()
    Storage = scrapy.Field()
    Ram = scrapy.Field()
    Display = scrapy.Field()
    Graphics = scrapy.Field()



