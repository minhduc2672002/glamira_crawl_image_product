import concurrent.futures
import time
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import sys

class Crawler:
    def __init__(self, urls_path, urls_checklist,image_path, log_file='crawler.log',max_woker = 3):
        self.urls_path = urls_path
        self.urls_checklist = urls_checklist
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.checklist_df = self.create_checklist()
        self.max_woker = max_woker
        self.image_path = image_path

    def polite_request(self, url):
        while True:
            try:
                response = requests.get(url)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException:
                print("Network disconnection!!!")
                time.sleep(5)

    def get_totalpage_totalproducts(self, url):
        response = self.polite_request(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        products = soup.select_one('ol.products.list.items.product-items li')
        total_page = int(products.get('data-lastpage'))
        total_product = int(products.get('data-total-items'))
        return total_page, total_product

    def get_infor(self, url):
        response = self.polite_request(url)
        if response and response.status_code == 200:
            logging.info("Request Success url: %s", url)
            soup = BeautifulSoup(response.content, 'html.parser')
            product_items = soup.find("ol", class_="products list items product-items")
            list_product_name = [element.text.strip() for element in product_items.find_all('h2', class_='product-item-details product-name')]
            list_product_image = [element.get('src') for element in product_items.find_all('img', class_='product-image-photo')]
            return list(zip(list_product_name, list_product_image))
        else:
            logging.warning("Request Failed url:%s", url)
            return [(None,None)]

    def save_to_csv(self, data, file_path, mode='a', header=False):
        df = pd.DataFrame(data)
        df.to_csv(file_path, mode=mode, index=False, header=header)

    def crawl_all_products(self, url, file_path):
        logging.info("---------------------------------------------------")
        logging.info(f"START SCRAP URL: {url} ")

        total_page, total_product = self.get_totalpage_totalproducts(url)
        if total_page and total_product:
            urls = [url + f'?p={i}' if i > 1 else url for i in range(1, total_page + 1)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_woker) as executor:
                futures = [executor.submit(self.get_infor, url) for url in urls]
                for future in concurrent.futures.as_completed(futures):
                    data = future.result()
                    self.save_to_csv(data, file_path)
            logging.info(f"Scraping Complete  {total_product} product of url: {url} ")
            return total_product
        return 0

    def process_data(self, file_path):
        df = pd.read_csv(file_path, header=None)
        df.drop_duplicates(inplace=True)
        df.to_csv(file_path, index=False, header=False)

    def create_checklist(self):
        try:
            if not os.path.exists(self.urls_checklist):
                urls_df = pd.read_csv(self.urls_path)
                checklist_df = urls_df
                checklist_df['status'] = 'NOT COMPLETE'
                checklist_df.to_csv(self.urls_checklist, header=True, index=False)
                print("Đã tạo checklist")
            else:
                checklist_df = pd.read_csv(self.urls_checklist)
                print("Đã load checklist")
            return checklist_df
        except Exception:
            print(f"Dữ liệu trong {self.urls_path} không đúng định dạng")
            sys.exit(1)

    def update_status(self, index, status):
        self.checklist_df.loc[index, 'status'] = status
        self.checklist_df.to_csv(self.urls_checklist, index=False)
        return status

    def download_image(self, image_url, save_path):
        response = self.polite_request(image_url)
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                file.write(response.content)
            logging.info(f"Photos have been downloaded and stored at: {save_path}")
        else:
            logging.info(f"Error: Unable to load image from {image_url}")

    def download_image_if_not_exists(self, link, product_name, product_image_folder):
        name = f"{product_name.strip().replace(' ', '-')}{link.split('/')[-1]}"
        save_path = os.path.join(product_image_folder, name)
        if not os.path.exists(save_path):
            self.download_image(link, save_path)
            return f"Dowloaded {name}"
        return f"Image {name} exist"

    def crawl_image(self, file_path):
        df = pd.read_csv(file_path, header=None)
        df.columns = ['product_name', 'image_link']

        product_image_folder = os.path.join(self.image_path, file_path.split('\\')[-1].split('.')[0])

        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)
        if not os.path.exists(product_image_folder):
            os.makedirs(product_image_folder)

        logging.info("---------------------------------------------------")
        logging.info(f"START DOWNLOAD IMAGE IN FILE: {file_path} ")
        links = df['image_link'].tolist()
        product_names = df['product_name'].tolist()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_woker) as executor:
            futures = [executor.submit(self.download_image_if_not_exists, link, product_name, product_image_folder) for link, product_name in zip(links, product_names)]
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                print(data)

    def run(self):
        total_product = 0
        for index, row in self.checklist_df.iterrows():
            url = row['url']
            status = row['status']
            if status == 'NOT COMPLETE':
                status = self.update_status(index, 'DOING')
            if status == 'DOING':
                name_file = url.split('/')[-2]
                file_path = os.path.join(os.getcwd(), 'data', f'{name_file}.csv')
                start_time = time.time()
                total_product += self.crawl_all_products(url, file_path)
                self.process_data(file_path)
                self.crawl_image(file_path)
                logging.info(f"------Crawl product at {url} complete.Processing times completed in {time.time() - start_time} seconds ---")
                status = self.update_status(index, 'DONE')
        logging.info("*****************************************")
        logging.info("Crawl Session Finish")
        print(f'Đã crawl {total_product} product!!')


if __name__ == "__main__":
    urls_path = os.path.join(os.getcwd(), 'data', 'urls_test.csv')
    urls_checklist = os.path.join(os.getcwd(), 'urls_checklist.csv')
    image_path = os.path.join(os.getcwd(), 'data', 'image')
    crawler = Crawler(urls_path, urls_checklist,image_path)
    crawler.run()