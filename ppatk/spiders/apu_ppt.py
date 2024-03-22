from datetime import datetime
import scrapy
import s3fs
import time


class ApuPptSpider(scrapy.Spider):
    name = "apu_ppt"
    allowed_domains = ["www.ppatk.go.id"]
    start_urls = ["https://www.ppatk.go.id/link/read/1249/buletin-statistik-apu-ppt.html"]
    custom_settings = {
        "USER_AGENT" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        for list in response.css('#blog > div > div > div.post-item-description > div:nth-child(3) > ul > li'):
            list_link = list.css('a::attr(href)').get()
            if list_link.split('/')[-1].split('.')[-1] == 'pdf':
                # time.sleep(2)
                yield scrapy.Request(url=list_link, callback=self.download_file)
            elif list_link.split('/')[-1].split('.')[-1] == 'html':
                yield scrapy.Request(url=list_link, callback=self.second_parse)
            
    def second_parse(self, response):
        pdf_link = response.css('#blog > div > div > div.post-image > a > img::attr(src)').get()
        ext = pdf_link.split('.')[-1]
        pdf_link = pdf_link.replace(ext, 'pdf')
        yield scrapy.Request(url=pdf_link, callback=self.download_file)

    def download_file(self, response):  
        crawling_epoch = int(datetime.now().timestamp())
        file_name = response.url.split('/')[-1]
        local_path = f'F:/Work/Garapan gweh/html/ppatk/pdf/{crawling_epoch}_{file_name}'
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
                
