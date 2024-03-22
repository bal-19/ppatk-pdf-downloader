from datetime import datetime
import scrapy
import s3fs
import time

class DatasetSpider(scrapy.Spider):
    name = "dataset"
    allowed_domains = ["satudata.ppatk.go.id"]
    start_urls = ["https://satudata.ppatk.go.id/dataset/"]
    custom_settings = {
        "USER_AGENT" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        for list in response.css('#content > div.row.wrapper > div > section:nth-child(1) > div.module-content > ul > li > ul > li'):
            list_link = list.css('a::attr(href)').get()
            if list.css('a::text').get() == "XLSX":
                yield scrapy.Request(url=response.urljoin(list_link), callback=self.parse_list)
        
        next_pagination = response.css('#content > div.row.wrapper > div > section:nth-child(1) > div.pagination-wrapper > ul > li > a::attr(href)').extract()[-1]
        if next_pagination is not None:
            yield scrapy.Request(url=response.urljoin(next_pagination), callback=self.parse)
        
            
    def parse_list(self, response):
        for list in response.css('#dataset-resources > ul > li > div > ul > li:nth-child(2) > a'):
            list_link = list.css('a::attr(href)').get()
            yield scrapy.Request(url=response.urljoin(list_link), callback=self.download_file)
            
            
    def download_file(self, response):  
        crawling_epoch = int(datetime.now().timestamp())
        file_name = response.url.split('/')[-1]
        local_path = f'F:/Work/Garapan gweh/html/ppatk/xlsx/{crawling_epoch}_{file_name}'
        with open(local_path, 'wb') as f:
            f.write(response.body)
            
    def upload_to_s3(self, local_path, s3_path):
        client_kwargs = {
            'key': 'YOUR KEY',
            'secret': 'YOUR SECRET',
            'endpoint_url': 'YOUR ENDPOINT',
            'anon': False
        }
        
        # Buat instance S3FileSystem
        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file ke S3
        s3.upload(rpath=s3_path, lpath=local_path)