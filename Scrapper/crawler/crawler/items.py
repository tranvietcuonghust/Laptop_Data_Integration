import scrapy

# class CrawlerItem(scrapy.Item):
#     # define the fields for your item here like:
    
#     User = scrapy.Field()
#     Comment = scrapy.Field()
#     Time = scrapy.Field()


class TrungTranItem(scrapy.Item):
    URL = scrapy.Field()
    ImgURL = scrapy.Field()
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
    Status = scrapy.Field()
    
class Laptop88Item(scrapy.Item):
    ImgURL = scrapy.Field()
    URL = scrapy.Field()
    ImgURL =scrapy.Field()
    Name = scrapy.Field()
    Price = scrapy.Field()
    CPU = scrapy.Field()
    Storage = scrapy.Field()
    Ram = scrapy.Field()
    Display = scrapy.Field()
    Graphics = scrapy.Field()
    Status = scrapy.Field()

class LaptopTCCItem(scrapy.Item):
    URL = scrapy.Field()
    Name = scrapy.Field()
    Price = scrapy.Field()
    CPU = scrapy.Field()
    Storage = scrapy.Field()
    Ram = scrapy.Field()
    Display = scrapy.Field()
    Graphics = scrapy.Field()
    Status =scrapy.Field()

class PhucAnhItem(scrapy.Item):
    URL = scrapy.Field()
    Name = scrapy.Field()
    Price = scrapy.Field()
    CPU = scrapy.Field()
    Storage = scrapy.Field()
    Ram = scrapy.Field()
    Display = scrapy.Field()
    Graphics = scrapy.Field()
    CPU_code = scrapy.Field()
    CPU_speed = scrapy.Field()
    Num_cores = scrapy.Field()
    Num_thread = scrapy.Field()
    Ram_type = scrapy.Field()
    Ram_slot = scrapy.Field()
    Ram_speed = scrapy.Field()
    Storage_type = scrapy.Field()
    Storage_drive = scrapy.Field()
    Resolutions = scrapy.Field()
    Name = scrapy.Field()
    Wireless = scrapy.Field()
    Size = scrapy.Field()
    Weight = scrapy.Field()
    Color = scrapy.Field()
    Ports = scrapy.Field()
    OS = scrapy.Field()
    Battery = scrapy.Field()
    ImgURL = scrapy.Field()
    





