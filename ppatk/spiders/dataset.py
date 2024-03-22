import scrapy


class DatasetSpider(scrapy.Spider):
    name = "dataset"
    allowed_domains = ["satudata.ppatk.go.id"]
    start_urls = ["https://satudata.ppatk.go.id/dataset/"]
    custom_settings = {
        "USER_AGENT" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        print(response.url)
