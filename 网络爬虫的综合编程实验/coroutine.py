import asyncio
import json
import time
import aiohttp
from config import *
from pymongo import MongoClient
from single_thread import Scraper

class CoroutineScraper:
    @staticmethod
    async def get_homepage(category_id, page, page_size):
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
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with await session.post(url=url, data=jsondata, headers=headers) as response:
                resp = await response.text()
        resp_json = json.loads(resp)
        return resp_json['result']

    @staticmethod
    async def get_detailpage(item_id):
        url = f"https://apipc-xiaotuxian-front.itheima.net/goods?id={item_id}"
        headers = {
            "user-agent": "HomeworkCrawler/1.0",
        }
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with await session.get(url=url, headers=headers) as response:
                resp = await response.text()
        resp_json = json.loads(resp)
        return item_id, resp_json['result']

    @staticmethod
    async def get_item_picture(item_id, url):
        pic_name = url.split("/")[-1]
        cache_path = f"cache/pic_{pic_name}.cache"
        conn = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async with await session.get(url=url) as response:
                resp_content = await response.content.read()
        with open(f"picture/{pic_name}", 'wb') as f:
            f.write(resp_content)
        return item_id, pic_name

    @staticmethod
    def analyze_item(homepage_info: dict, detail_info: dict, pic: str):
        return Scraper.analyze(homepage_info, detail_info, pic)

    @staticmethod
    async def crawl_and_store(category_id, num_pages, connection):
        all_items = []
        item_detail = {}
        pics = {}
        homepage_tasks = []
        for i in range(1, num_pages + 1):
            homepage_tasks.append(
                asyncio.create_task(
                    CoroutineScraper.get_homepage(category_id, i, 20)
                )
            )
        await asyncio.gather(*homepage_tasks)
        for i in homepage_tasks:
            ires = i.result()
            all_items.extend(ires['items'])

        detail_tasks = []
        pic_tasks = []
        for item in all_items:
            item_id = item['id']
            pic_url = item['picture']
            detail_tasks.append(
                asyncio.create_task(
                    CoroutineScraper.get_detailpage(item_id)
                )
            )
            pic_tasks.append(
                asyncio.create_task(
                    CoroutineScraper.get_item_picture(item_id, pic_url)
                )
            )
        await asyncio.gather(*detail_tasks)
        await asyncio.gather(*pic_tasks)
        for i in detail_tasks:
            ires = i.result()
            item_detail[ires[0]] = ires[1]
        for i in pic_tasks:
            ires = i.result()
            pics[ires[0]] = ires[1]
        for item in all_items:
            connection.insert_one(CoroutineScraper.analyze_item(item, item_detail[item['id']], pics[item['id']]))

    @staticmethod
    def run(category_id, num_pages, connection):
        loop = asyncio.get_event_loop()
        coroutine = asyncio.ensure_future(
            CoroutineScraper.crawl_and_store(category_id, num_pages, connection)
        )
        loop.run_until_complete(asyncio.gather(coroutine))


if __name__ == '__main__':
    start = time.time()
    category = "109243036"  
    client = MongoClient('mongodb://localhost:27017')
    db = client['week7']
    connection = db['week7']
    CoroutineScraper.run(category, 2, connection)
    end = time.time()
    print(f"协程用时: {end-start}s")

