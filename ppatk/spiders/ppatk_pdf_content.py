from datetime import datetime
import scrapy
import s3fs
import json
import time


class PpatkPdfContentSpider(scrapy.Spider):
    name = "ppatk_pdf_content"
    allowed_domains = ["www.ppatk.go.id"]
    start_urls = ["https://www.ppatk.go.id/publikasi/read/214/penilaian-risiko-sektoral-tindak-pidana-pencucian-uang-dan-pendanaan-terorisme-pada-teknologi-finansial-tahun-2023.html"]
    custom_settings = {
        "USER_AGENT" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    def parse(self, response):
        link = response.url
        domain = link.split('/')[2]
        source = "ppatk"
        data_name = "Knowledge terkait modus, instrumen rawan, assesment resiko, serta contoh kasus TPPU-TPPT di Indonesia dan Luar Negeri"
        
        img = response.css('#blog > div > div > div.post-image > a > img::attr(src)').get()
        ext_img = img.split('.')[-1]
        title = response.css('#blog > div > div > div.post-item-description > h2::text').get()
        rating = response.css('#blog > div > div > div.post-item-description > div.ratingStar > span > span > span > span > span > span > span > span > span > span::text').get()
        rating = int(rating.replace(' | ', ''))
        post_date = response.css('#blog > div > div > div.post-item-description > div.post-meta > span.post-meta-date::text').get()
        # content = response.xpath('/html/body/div[8]/div[2]/div[1]/section/div/div/div[1]/div/div/div/div[2]/div[3]/text()').get()
        content = "Penilaian Risiko Sektoral TPPU dan TPPT pada Teknologi Finansial (Tekfin) Tahun 2023 ini penting, adanya perkembangan teknologi informasi dan tuntutan hidup yang serba cepat menimbulkan perubahan gaya hidup di masyarakat. Perkembangan teknologi tersebut banyak membantu masyarakat dalam memenuhi kebutuhannya termasuk akses terhadap layanan finansial. Namun, di sisi lain perkembangan teknologi tersebut memiliki potensi risiko yang jika tidak dimitigasi dengan baik dapat mengganggu integritas sistem keuangan. Rekomendasi 15 FATF menyebutkan bahwa Lembaga keuangan harus memberi perhatian khusus terhadap ancaman tindak pidana pencucian uang yang dapat timbul dari teknologi baru atau yang sedang berkembang yang mendukung anonimitas, dan mengambil tindakan untuk mencegah penggunaan teknologi tersebut dalam pencucian uang jika diperlukan. Guna memitigasi risiko yang dapat muncul dari pemanfaatan tekfin dalam pencucian uang dan pendanaan terorisme, salah satu langkah yang dilakukan adalah menyusun penilaian risiko sektoral. Penilaian sektoral diharapkan dapat memberikan pemahaman yang lebih sehingga dari berbagai faktor yang terdapat dalam tekfin dapat diketahui hal mana yang paling berisiko sehingga dapat dilakukan mitigasi secara efektif dan efisien. Proses penilaian risiko sektoral ini memperhitungkan faktor risiko TPPU dan TPPT yang mencakup ancaman, kerentanan dan dampak. Penilaian risiko sektoral ini disusun dengan menggunakan mixed method research berdasarkan data historis dan expert view yang ditujukan untuk mengidentifikasi, menganalisis, mengevaluasi, dan memitigasi risiko TPPU dan TPPT. Identifikasi dan analisis risiko TPPU pada jenis-jenis Teknologi Finansial di Indonesia berdasarkan Tindak Pidana Asal (TPA) TPPU, wilayah atau provinsi, profil pelaku TPPU dan TPPT, dan tipologi TPPU dan TPPT."
        pdf_link = img.replace(ext_img, 'pdf')
        crawling_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        crawling_epoch = int(datetime.now().timestamp())
        data = {
            "link" : link,
            "domain" : domain,
            "source" : source,
            "data_name" : data_name,
            "img" : img,
            "title" : title,
            "rating" : rating,
            "post_date" : post_date,
            "content" : content,
            "pdf_link" : pdf_link,
            "crawling_time" : crawling_time,
            "crawling_epoch" : crawling_epoch
        }
        
        with open(f"json/data_{crawling_epoch}.json", 'w') as outfile:
            json.dump(data, outfile)
            
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