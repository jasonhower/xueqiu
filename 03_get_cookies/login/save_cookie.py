# 数据存到cookie池
import pymongo
import time
from login.save_cookie import *

def save_cookie(cookie,agent):
    client = pymongo.MongoClient('192.168.1.108', connect=False)
    db = client['cookiesPool']
    collection = db['cookies']
    data = {
        "Host": "xueqiu.com",
        "Upgrade-Insecure-Requests": "1",
        "Cookie": cookie,
        "User-Agent": agent,
        "time": int(time.time()),
        "count_used": 0
    }
    return collection.insert(data)