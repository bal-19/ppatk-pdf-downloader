from ppatk.items import DttotItem
from datetime import datetime
import scrapy
import s3fs
import json
import time
import os

class DttotSpider(scrapy.Spider):
    name = "dttot"
    allowed_domains = ["www.ppatk.go.id"]
    start_urls = ["https://www.ppatk.go.id/link/read/23/dttot-proliferasi-wmd.html"]
    custom_settings = {
        "USER_AGENT" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        #blog > div > div > div.post-item-description > div:nth-child(3) > ul:nth-child(4) > li > a     FIRST UL
        #blog > div > div > div.post-item-description > div:nth-child(3) > ul:nth-child(8) > li > a     SECOND UL
        #blog > div > div > div.post-item-description > div:nth-child(3) > p:nth-child(10) > a          THIRD UL
        
        # first list
        for first_list in response.css('#blog > div > div > div.post-item-description > div:nth-child(3) > ul:nth-child(4) > li'):
            first_list_link = first_list.css('a::attr(href)').get()
            first_list_link = first_list_link.replace('https://www.', '')
            first_list_link = f"https://www.{first_list_link}"
            if first_list_link.split('/')[-1].split('.')[-1] == 'pdf':
                time.sleep(2)
                yield scrapy.Request(url=first_list_link, callback=self.download_file)
                
        # second list
        for second_list in response.css('#blog > div > div > div.post-item-description > div:nth-child(3) > ul:nth-child(8) > li'):
            second_list_link = second_list.css('a::attr(href)').get()
            second_list_link = second_list_link.replace('https://www.', '')
            second_list_link = f"https://www.{second_list_link}"
            if second_list_link.split('/')[-1].split('.')[-1] == 'pdf':
                time.sleep(2)
                yield scrapy.Request(url=second_list_link, callback=self.download_file)

        # third list
        for third_list in response.css('#blog > div > div > div.post-item-description > div:nth-child(3) > p:nth-child(10) > a'):
            third_list_link = third_list.css('::attr(href)').get()
            third_list_link = third_list_link.replace('https://www.', '')
            third_list_link = f"https://www.{third_list_link}"
            if third_list_link.split('/')[-1].split('.')[-1] == 'pdf':
                time.sleep(2)
                yield scrapy.Request(url=third_list_link, callback=self.download_file)

    def download_file(self, response):  
        crawling_epoch = int(datetime.now().timestamp())
        file_name = response.url.split('/')[-1]
        local_path = f'F:/Work/Garapan gweh/html/ppatk/pdf/{crawling_epoch}_{file_name}.pdf'
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