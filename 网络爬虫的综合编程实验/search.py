from pymongo import MongoClient
import time

client = MongoClient('mongodb://localhost:27017')
db = client['week7']
collection = db['week7']

def query_data():
    query = collection.find({}, {'name': 1, 'price': 1, 'desc': 1})
    for item in query:
        print(item)

def create_index():
    collection.create_index([('name', 1)])  # 在商品名称字段上创建升序索引

def test_query_speed():
    start = time.time()
    query_data()
    end = time.time()
    print(f"查询数据用时: {end-start}s")

if __name__ == '__main__':
    print("开始查询数据")
    test_query_speed()
    print("开始创建索引")
    create_index()
    print("索引创建完成")
    print("再次查询数据")
    test_query_speed()
