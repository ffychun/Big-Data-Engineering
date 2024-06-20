import time
from threading import Lock, Thread
import config
from single_thread import Scraper
import queue  
from pymongo import MongoClient


class MultithreadScraper:
    def __init__(self, collection, crawl_pages=2, category_id=109243036):
        self._homepage_queue = queue.Queue()
        self._detail_info_queue = queue.Queue()
        self._picture_queue = queue.Queue()
        self._base_info_lock = Lock()
        self._base_info = []
        self._pic_info_lock = Lock()
        self._pic_info = {}
        self._detail_info_lock = Lock()
        self._detail_info = {}
        self._collection = collection
        self._crawl_pages = crawl_pages
        self._crawl_category_id = category_id

    def _add_work_to_homepage_queue(self):
        for i in range(1, self._crawl_pages + 1):
            self._homepage_queue.put((self._crawl_category_id, i, 20))

    def work_homepage_queue(self):
        param = self._homepage_queue.get()
        result = Scraper.get_homepage(*param)
        time.sleep(0.5)
        for i in result['items']:
            self._picture_queue.put((i['id'], i['picture']))
            self._detail_info_queue.put((i['id'],))
            with self._base_info_lock:
                self._base_info.append(i)

    def work_picture_queue(self):
        param = self._picture_queue.get()
        result = Scraper.get_goods_picture(param[1])
        time.sleep(0.5)
        with self._pic_info_lock:
            self._pic_info[param[0]] = result

    def work_detail_queue(self):
        param = self._detail_info_queue.get()
        result = Scraper.get_detailpage(param[0])
        time.sleep(0.5)
        with self._detail_info_lock:
            self._detail_info[param[0]] = result

    def _write_to_db(self):
        for i in self._base_info:
            id = i['id']
            data = Scraper.analyze(
                homepage_info=i,
                detail_info=self._detail_info[id],
                pic=self._pic_info[id]
            )
            self._collection.insert_one(data)

    def exec(self):
        self._add_work_to_homepage_queue()
        homepage_threads = []
        for i in range(self._homepage_queue.qsize()):
            homepage_threads.append(Thread(target=self.work_homepage_queue))
        for i in homepage_threads:
            i.start()
        for i in homepage_threads:
            i.join()

        pic_detail_threads = []
        for i in range(self._picture_queue.qsize()):
            pic_detail_threads.append(Thread(target=self.work_picture_queue))
        for i in range(self._detail_info_queue.qsize()):
            pic_detail_threads.append(Thread(target=self.work_detail_queue))
        for i in pic_detail_threads:
            i.start()
        for i in pic_detail_threads:
            i.join()

        self._write_to_db()


if __name__ == '__main__':
    start = time.time()
    client = MongoClient('mongodb://localhost:27017')
    db = client['week7']
    collection = db['week7']
    multicrawler = MultithreadScraper(collection)
    multicrawler.exec()
    end = time.time()
    print(f"多线程用时: {end-start}s")
