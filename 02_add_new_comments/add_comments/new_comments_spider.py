# coding:utf-8
# 增量爬评论数据
import json
import time
import pymongo
from requests.exceptions import ConnectionError
import re
import MySQLdb
import requests
import datetime
from add_comments.config import *
from urllib.parse import urlencode
from json.decoder import JSONDecodeError


proxy = None


class add_comments():
    def __init__(self, type, databases_group):
        self.type = type
        self.header = self.get_header()
        self.databases_group = databases_group


    # 获取存放到的Mysql数据库
    def get_mysql_databases(self):
        # 讨论 : 1 ; 交易 : 2 ; 新闻 : 3 ; 公告 : 4 ; 研报 : 5
        if self.type == 1:
            return TAOLUN_DB_MYSQL
        elif self.type == 2:
            return JIAOYI_DB_MYSQL
        elif self.type == 3:
            return XINWEN_DB_MYSQL
        elif self.type == 4:
            return GONGGAO_DB_MYSQL
        elif self.type == 5:
            return YANBAO_DB_MYSQL
        else:
            return None

    # 从cookie池获取cookie
    def get_header(self):
        client_cookie = pymongo.MongoClient(MONGODB_URL, connect=False)
        db_cookie = client_cookie[COOKIE_DB]
        collection_cookie = db_cookie[COOKIE_TABLE]
        count = collection_cookie.count()
        if count < 5:
            print("cookie池的数量小于5,当前为", count)
        cursor = collection_cookie.find(no_cursor_timeout=True)
        for item in cursor:
            now_time = int(time.time())
            if int(item['time']) + 172800 > now_time:  # 超过48小时
                if item['count_used'] < 3:
                    headers = {
                        "Cookie": item['Cookie'],
                        "Host": "xueqiu.com",
                        "Upgrade-Insecure-Requests": "1",
                        "User-Agent": item['User-Agent']
                    }
                    # 将count + 1
                    collection_cookie.update({'_id': item['_id']}, {'$set': {'count_used': item['count_used'] + 1}})
                    print(headers)
                    return headers
                else:
                    pass
            else:
                print(item['_id'], "超过时间点了！")
            # 将这条信息删除
            # collection_cookie.remove({"_id": item['_id']})
        cursor.close()


    # 获取页面信息
    def comments_get_page_index(self,comment_id,count = 10,page =1):
        data = {
            'id': comment_id,
            'count': count,
            'page': page,
            'reply': 'true',
            'type': 'status',
            'split': 'true',
            'asc': 'false'
        }
        params = urlencode(data)
        base = 'https://xueqiu.com/statuses/comments.json'
        url = base + '?' + params
        return self.get_html(url)


    def parse_page_index(self,html):
        try:
            data = json.loads(html)
            if data and 'comments' in data.keys():
                for item in data.get('comments'):
                    yield item
        except JSONDecodeError:
            pass


    # 采用正则表达式获取具体的信息
    def comments_parse_page_detail(self,html):
        datas = []
        items = self.parse_page_index(html)
        for it in items:
            time = self.deal_data(str(it['created_at']))
            people = it['user']['screen_name']
            content = it['text']
            try:
                content_pattern = re.compile(r'<[^>]+>', re.S)
                content = content_pattern.sub('', content)
                content = content.replace("\u0026", "").replace("nbsp;", "")
            except AttributeError:
                pass
            data = {
                '评论人': people,
                '评论时间': time,
                '评论内容': content
            }
            datas.append(data)
        return datas


    # 处理日期 1515026126000
    # 将UNIX时间戳转化成标准时间戳
    def deal_data(self,data):
        data = int(data[:-3])
        format = '%Y-%m-%d %H:%M:%S'
        value = time.localtime(data)
        dt = time.strftime(format, value)
        return dt


    def get_comments(self,comment_id):
        comments = []
        ret_json = self.comments_get_page_index(comment_id)
        data = json.loads(ret_json)
        count = data.get('count')
        if count != 0:
            i = int(count / 10) + 1
            for x in range(1, i + 1):
                re_json = self.comments_get_page_index(comment_id, count=10, page=x)
                res = self.comments_parse_page_detail(re_json)
                for r in res:
                    comments.append(r)
        return comments


    def deal_url(self,url):
        if self.type == 3 or self.type == 4 or self.type == 5:
            comment_id = re.findall(re.compile('https://xueqiu.com/S/.*?/(\d+)', re.S), str(url))
        else:
            comment_id = re.findall(re.compile('https://xueqiu.com/\d+/(\d+)', re.S), str(url))
        return comment_id[0]


    def get_html(self,url, count=1):
        global proxy
        if count >= 5:
            print('Tried Too Many Counts')
            return None
        try:
            if proxy:
                proxies = {
                    'http': 'http://' + proxy
                }
                print(proxies)
                response = requests.get(url, allow_redirects=False, headers=self.header, proxies=proxies)
            else:
                response = requests.get(url, allow_redirects=False, headers=self.header)
            if response.status_code == 200:
                return response.text
            if response.status_code == 302:
                # Need Proxy
                print('302')
                proxy = self.get_proxy()
                if proxy:
                    print('Using Proxy', proxy)
                    return self.get_html(url)
                else:
                    print('Get Proxy Failed')
                    return None
        except ConnectionError as e:
            print('Error Occurred', e.args)
            proxy = self.get_proxy()
            count += 1
            return self.get_html(url, count)

    def get_proxy(self):
        try:
            response = requests.get('http://127.0.0.1:5000/get')
            if response.status_code == 200:
                return response.text
            return None
        except ConnectionError:
            return None

    def start_MySQL(self):
        conn = MySQLdb.connect(
            host='192.168.1.108',
            port=3306,
            user='Andlinks',
            passwd='Andlinks2017',
            db='xueqiu',
            charset='utf8')

        cur = conn.cursor()
        myConn_list = [conn, cur]
        return myConn_list

    def close_MySQL(self,cur, conn):
        cur.close()
        conn.close()


    def main(self):
        database_groups = self.databases_group

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for db_name in database_groups:
            client = pymongo.MongoClient(MONGODB_URL, connect=False)
            db = client[db_name]
            for table in db.collection_names():
                if table != 'system.indexes':
                    collection = db[table]
                    cursor = collection.find(no_cursor_timeout=True)
                    for item in cursor:
                        try:
                            if item['count_key'] < 8:#  更新7天内数据
                                url = item['url']
                                name = item['股票名称']
                                _id = item['_id']
                                print("当前时间为：",time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"当前数据库为：", db_name, " 表为：", table,"url为：",url)
                                comment_id = self.deal_url(url)
                                comments = self.get_comments(comment_id)
                                # 重新将Mongodb中信息更改
                                collection.update_one({"_id": _id}, {"$set": {"评论": comments}})
                                collection.update_one({"_id": _id}, {"$set": {"评论量": str(len(comments))}})
                                # 将新的数据插入到Mysql中
                                for temple in comments:
                                    sql = "INSERT INTO " + self.get_mysql_databases() + "(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(url, name, temple['评论人'], temple['评论时间'], temple['评论内容'])
                                    print(sql)
                                    try:
                                        cur.execute(sql)
                                        conn.commit()
                                    except Exception as e:
                                        print(e)
                        except KeyError as e:
                            print(e)
                    cursor.close()
        self.close_MySQL(cur, conn)