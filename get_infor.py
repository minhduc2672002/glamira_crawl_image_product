import concurrent.futures
import time
import pandas as pd
import requests
import os
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import sys
def polite_request(url):
    '''
    Try to send the request again when network connection is lost 
    '''
    try:
        response = requests.get(url)
        return response
    except requests.exceptions.RequestException as e:
        print("Network disconnection!!!")
        time.sleep(5)
        return polite_request(url)
    
def get_totalpage_totalproducts(url):
    '''
    Get total pages and total products at url
    '''    
    response = polite_request(url)
    if response and response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        products = soup.select_one('ol.products.list.items.product-items li')
        total_page = int(products.get('data-lastpage'))
        total_product = int(products.get('data-total-items'))
        return total_page,total_product
    else:
        return None,None
    
def get_infor(url):
    '''
    Get product information including product name and image link
    '''
    list_result= []
    response = polite_request(url)
    
    if response and response.status_code == 200:
        logging.info("Request Success url: %s", url)
        soup = BeautifulSoup(response.content, 'html.parser')
        product_items  = soup.find("ol",class_="products list items product-items")
        list_product_name = [element.text.strip() for element in product_items.find_all('h2',class_='product-item-details product-name')]
        list_product_image = [element.get('src') for element in product_items.find_all('img',class_='product-image-photo')]
        pairs = list(zip(list_product_name, list_product_image))
        list_result.extend(pairs)
        return list_result
    else:
        logging.warning("Request Failed url:%s", url)


def save_to_csv(data, file_path, mode='a', header=False):
    df = pd.DataFrame(data)
    df.to_csv(file_path, mode=mode, index=False, header=False)


def crawl_all_products(url,file_path):
    logging.info("---------------------------------------------------")
    logging.info(f"START SCRAP URL: {url} ")

    total_page,total_product = get_totalpage_totalproducts(url)
    urls = [url + f'?p={i}' if i > 1 else url for i in range(1,total_page+1) ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit tasks to the executor
            futures = [executor.submit(get_infor,url) for url in urls]
            # Ensure all threads have completed their tasks
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                save_to_csv(data,file_path)
    logging.info(f"Scraping Complete  {total_product} product of url: {url} ")
    return total_product

def process_data(file_path):
    df = pd.read_csv(file_path, header=None)
    df.drop_duplicates(inplace=True)
    df.to_csv(file_path,index=False, header=False)

def create_checklist(urls_path,urls_checklist):
    try:
        checklist_df = None
        if not os.path.exists(urls_checklist):
            urls_df = pd.read_csv(urls_path)
            checklist_df = urls_df
            checklist_df['status']='NOT COMPLETE'
            checklist_df.to_csv(urls_checklist,header=True,index=False)
            print("Đã tạo checklist")
        else:
            checklist_df = pd.read_csv(urls_checklist)
            print("Đã load checklist")
        return checklist_df
    except Exception as e:
        print(f"Dữ liệu trong {urls_path} không đúng định dạng")
        sys.exit(1)

def update_status(df,file_path,index,status):
    df.loc[index,'status'] = status
    df.to_csv(file_path,index=False)
    return status

def download_image(image_url,save_path):
    response = polite_request(image_url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            # Ghi dữ liệu nhị phân vào file
            file.write(response.content)
        logging.info(f"Photos have been downloaded and stored at: {save_path}")
    else:
        logging.info(f"Error: Unable to load image from {image_url}")
def download_image_if_not_exists(link, product_name, product_image_folder):
    name = f"{product_name.strip().replace(' ', '-')}{link.split('/')[-1]}"
    save_path = os.path.join(product_image_folder, name)
    if not os.path.exists(save_path):
        download_image(link, save_path)
        return f"Dowloaded {name}"
    return f"Image {name} exist"

def crawl_image(file_path):
    df = pd.read_csv(file_path,header=None)
    df.columns = ['product_name','image_link']

    image_folder = os.getcwd() + r'\data\images'
    product_image_folder = file_path.split('\\')[-1].split('.')[0]
    product_image_folder =os.getcwd() +os.path.join('\data\images',product_image_folder)

    if not os.path.exists(image_folder):
        os.makedirs(image_folder)
    if not os.path.exists(product_image_folder):
        os.makedirs(product_image_folder)

    logging.info("---------------------------------------------------")
    logging.info(f"START DOWNLOAD IMAGE IN FILE: {file_path} ")
    links = df['image_link'].tolist()
    product_names = df['product_name'].tolist()


    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(download_image_if_not_exists, link, product_name, product_image_folder) for link, product_name in zip(links, product_names)]
            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                print(data)
def main():
    logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    urls_path = os.getcwd() + r'\data\urls_test.csv'
    urls_checklist= os.getcwd() + r'\urls_checklist.csv'

    checklist_df  = create_checklist(urls_path,urls_checklist)
    total_product = 0
    for index,row in checklist_df.iterrows():
        url = row['url']
        status = row['status']
        if status == 'NOT COMPLETE':
            status = update_status(checklist_df,urls_checklist,index,'DOING')
        if status == 'DOING':
            name_file = url.split('/')[-2]
            file_path =os.getcwd()+os.path.join('\data', f'{name_file}.csv')

            start_time = time.time()
            total_product += crawl_all_products(url,file_path)
            process_data(file_path)
            crawl_image(file_path)
            logging.info(f"------Crawl product at {url} complete.Processing times completed in {time.time() - start_time} seconds ---")
            status = update_status(checklist_df,urls_checklist,index,'DONE')
    logging.info("*****************************************")
    logging.info("Crawl Session Finish")
    print(f'Đã crawl {total_product} product!!')
if __name__ == "__main__":
    main()