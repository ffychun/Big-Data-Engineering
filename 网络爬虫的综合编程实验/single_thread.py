import json
import time
from pymongo import MongoClient
from config import *
import requests

class Scraper:
    @staticmethod
    def get_homepage(category_id, page, page_size):
        url = "https://apipc-xiaotuxian-front.itheima.net/category/goods/temporary"
        headers = {
            "content-type": "application/json",
            "user-agent": "HomeworkCrawler/1.0",
        }
        data = {
            'categoryId': str(category_id),
            'page': str(page),
            'pageSize': str(page_size)
        }
        jsondata = json.dumps(data)
        resp = requests.post(url=url, data=jsondata, headers=headers)
        resp_json = json.loads(resp.text)
        return resp_json['result']

    @staticmethod
    def get_detailpage(item_id):
        url = f"https://apipc-xiaotuxian-front.itheima.net/goods?id={item_id}"
        headers = {
            "user-agent": "HomeworkCrawler/1.0",
        }
        resp = requests.get(url, headers=headers)
        resp_json = json.loads(resp.text)
        return resp_json['result']

    @staticmethod
    def get_goods_picture(url):
        pic_name = url.split("/")[-1]
        resp = requests.get(url)
        with open(f"picture/{pic_name}", 'wb') as f:
            f.write(resp.content)
        return pic_name

    @staticmethod
    def analyze(homepage_info: dict, detail_info: dict, pic: str):
        item_info = {
            'id': int(homepage_info['id']),
            'url': f"https://erabbit.itheima.net/#/product/{homepage_info['id']}",
            'name': homepage_info['name'],
            'desc': homepage_info['desc'],
            'price': float(homepage_info['price']),
            'pic': pic,
            'detail': str(detail_info['details']['properties'])
        }
        return item_info

    @staticmethod
    def crawl_and_store(category_id, num, collection):
        all_items = []
        item_detail = {}
        pics = {}
        for i in range(1, (num+19)//20 + 1):
            res = None
            while res is None:
                res = Scraper.get_homepage(category_id, i, 20)
            all_items.extend(res['items'])
            print(f"Crawled Page {i} of {(num+19)//20}")
        for itemid, item in enumerate(all_items[:num]):
            detail = None
            pic = None
            while detail is None:
                detail = Scraper.get_detailpage(item['id'])
            while pic is None:
                pic = Scraper.get_goods_picture(item['picture'])
            item_detail[item['id']] = detail
            pics[item['id']] = pic
            print(f"Crawled Item {itemid + 1} of {num}")
        for item in all_items[:num]:
            analyzed_item = Scraper.analyze(item, item_detail[item['id']], pics[item['id']])
            collection.insert_one(analyzed_item) 

if __name__ == '__main__':
    print("开始爬取")
    start = time.time()
    category = "109243036"  
    client = MongoClient('mongodb://localhost:27017')
    db = client['week7']
    collection = db['week7']
    print("数据库连接成功")
    Scraper.crawl_and_store(category, 50, collection)
    end = time.time()
    print(f"程序用时: {end-start}s")
