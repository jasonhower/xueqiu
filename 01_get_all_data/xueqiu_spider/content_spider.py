# coding:utf-8
# 雪球
import re
import json
import time
import datetime
import pymongo
import random
import requests
from xueqiu_spider.config import *
from urllib.parse import urlencode
from json.decoder import JSONDecodeError
from requests.exceptions import ConnectionError

proxy = None
count = 0


class DealHtml(object):
    def __init__(self, type, divide_db, divide_collection, add_time):
        self.type = type
        self.header = self.get_header()
        self.databases = self.get_databases()            # 存储到的数据库
        self.past_time = int(time.time()) - add_time     # 过去时间点

        # 保存的数据库
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        self.db = client[self.databases]

        # 上市公司的表
        client_sjs = pymongo.MongoClient(MONGO_URL, connect=False)
        db_sjs = client_sjs[divide_db]
        self.collection_sjs = db_sjs[divide_collection]
        print('init end')

    # 获取存放到的数据库
    def get_databases(self):
        # 讨论 : 1 交易 : 2 新闻 : 3 公告 : 4 研报 : 5
        if self.type == 1:
            return TODAY_TAOLUN_DB
        elif self.type == 2:
            return TODAY_JIAOYI_DB
        elif self.type == 3:
            return TODAY_XINWEN_DB
        elif self.type == 4:
            return TODAY_GONGGAO_DB
        elif self.type == 5:
            return TODAY_YANBAO_DB
        else:
            return None

    # 从cookie池获取cookie
    def get_header(self):
        client_cookie = pymongo.MongoClient(MONGO_URL, connect=False)
        db_cookie = client_cookie['cookiesPool']
        collection_cookie = db_cookie['cookies']
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
                collection_cookie.remove({"_id": item['_id']})
        cursor.close()

    # 获取cookie池
    def get_proxy(self):
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200:
                return response.text
            return None
        except ConnectionError:
            return None

    # 获取 html
    def get_html(self, url, count=1):
        global proxy
        if count >= 5:
            print('Tried Too Many Counts')
            return None
        try:
            if proxy:
                proxies = {
                    'http': 'http://' + proxy
                }
                response = requests.get(url, allow_redirects=False, headers=self.header, proxies=proxies)
            else:
                response = requests.get(url, allow_redirects=False, headers=self.header)
            if response.status_code == 200:
                return response.text
            if response.status_code == 302:
                # Need Proxy
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

    # 根据不同网页获取 url
    def get_page_url(self,i,ID):
        if self.type == 1:
            data = {
                'count': 10,
                'comment': 0,
                'symbol': ID,
                'hl': 0,
                'source': 'user',
                'sort': 'time',
                'page': i
            }
            params = urlencode(data)
            base = 'https://xueqiu.com/statuses/search.json'
            url = base + '?' + params
            return url
        elif self.type == 2:
            data = {
                'count': 10,
                'comment': 0,
                'symbol': ID,
                'hl': 0,
                'source': 'trans',
                'sort': 'time',
                'page': i
            }
            params = urlencode(data)
            base = 'https://xueqiu.com/statuses/search.json'
            url = base + '?' + params
            return url
        elif self.type == 3:
            data = {
                'count': 10,
                'symbol_id': ID,
                'source': '自选股新闻',
                'page': i
            }
            params = urlencode(data)
            base = 'https://xueqiu.com/statuses/stock_timeline.json'
            url = base + '?' + params
            return url
        elif self.type == 4:
            data = {
                'count': 10,
                'symbol_id': ID,
                'source': '公告',
                'page': i
            }
            params = urlencode(data)
            base = 'https://xueqiu.com/statuses/stock_timeline.json'
            url = base + '?' + params
            return url
        elif self.type == 5:
            data = {
                'count': 10,
                'symbol_id': ID,
                'source': '研报',
                'page': i
            }
            params = urlencode(data)
            base = 'https://xueqiu.com/statuses/stock_timeline.json'
            url = base + '?' + params
            return url
        else:
            return None

    # 获取页面信息
    def get_page_index(self, i, ID):
        url = self.get_page_url(i,ID)
        if url:
            try:
                response = requests.get(url, headers=self.header)
                if response.status_code == 200:
                    return response.text
                return None
            except ConnectionError:
                print('Error occurred')
                return None

    # 获取页面具体信息
    def get_page_detail(self, url):
        return self.get_html(url)

    # 由JSON数据返回URl
    def parse_page_index(self, text):
        try:
            data = json.loads(text)
            if data and 'list' in data.keys():
                for item in data.get('list'):
                    yield item.get('target')
        except JSONDecodeError:
            pass

    # 采用正则表达式获取具体的信息
    def parse_page_detail(self, html, url, ID, Name):
        html = self.get_html_json(html)

        try:
            text_pattern = re.compile("id.*?user_id.*?\"title\":\"(.*?)\",\"created_at\":(\d+),\"retweet_count\":(\d+),\"reply_count\":(\d+),\"fav_count\":(\d+),.*?user.*?\"screen_name\":\"(.*?)\",.*?\"like_count\":(\d+),.*?\"is_answer.*?\"text\":\"(.*?)\",\"source\".*?}", re.S)
            text = re.findall(text_pattern, str(html))
            title = text[0][0] if text[0][0] else '无'
            time = self.deal_data(text[0][1])
            retweet_count = text[0][2]
            reply_count = text[0][3]
            screen_name = text[0][5]
            like_count = text[0][6]
            content = text[0][7]
            if self.type == 3 or self.type == 4:
                try:
                    out_url_pattern = re.compile(r'href=\\\\"(.*?)\\\\"\stitle', re.S)
                    out_url = re.findall(out_url_pattern, content)[0]
                except (AttributeError, IndexError):
                    out_url = None
            try:
                content_pattern = re.compile(r'<[^>]+>', re.S)
                content = content_pattern.sub('', content)
                content = content.replace("&nbsp;", "").replace("\u0026", "")
            except AttributeError:
                pass
        except IndexError:
            print('error')
            return None
        if self.type == 3:
            return {
                'A股代码': ID,
                '股票名称': Name,
                'url': url,
                '标题': title,
                '时间': time,
                '摘要': content,
                '发布者': screen_name,
                '外部链接': out_url,
                '转发量': retweet_count,
                '评论量': reply_count,
                '点赞量': like_count,
                'count_key': 1
            }
        elif self.type == 4:
            return {
                'A股代码': ID,
                '股票名称': Name,
                'url': url,
                '标题': title,
                '时间': time,
                '正文': content,
                '发布者': screen_name,
                'PDF下载链接': out_url,
                '转发量': retweet_count,
                '评论量': reply_count,
                '点赞量': like_count,
                'count_key': 1
            }
        else:
            return {
                'A股代码': ID,
                '股票名称': Name,
                'url': url,
                '标题': title,
                '时间': time,
                '正文': content,
                '发布者': screen_name,
                '转发量': retweet_count,
                '评论量': reply_count,
                '点赞量': like_count,
                'count_key': 1
            }

    # 提取 JSON
    def get_html_json(self, html):
        text_pattern = re.compile('window.SNOWMAN_STATUS\s=(.*?);\swindow.SNOWMAN_TARGET', re.S)
        text = re.findall(text_pattern, html)
        try:
            # data = json.loads(text[0])
            return text
        except JSONDecodeError:
            return None

    # 处理日期 1515026126000
    # 将UNIX时间戳转化成标准时间戳
    def deal_data(self, data):
        data = int(data[:-3])
        format = '%Y-%m-%d %H:%M:%S'
        value = time.localtime(data)
        dt = time.strftime(format, value)
        return dt

    # 判断是否出现异常
    def error_code(self, html):
        try:
            data = json.loads(html)
            if data and 'error_code' in data.keys():
                return data['error_code']
        except JSONDecodeError:
            pass

    # 保存到Mongodb中
    def save_to_mongodb(self, result, table_name):
        if self.db[table_name].insert(result):
            print('Successfully Saved to Mongodb', result['url'])
            return True
        return False

    # 获取最大页数
    def get_maxPage(self, text):
        try:
            data = json.loads(text)
            if data and 'maxPage' in data.keys():
                return data.get('maxPage')
        except JSONDecodeError:
            pass

    # 将每一条信息进行爬取
    def get_connent(self, ID, Name, i):
        start_url = XUEQIU_URL
        get_json = self.get_page_index(i, ID)
        if get_json:
            if self.error_code(get_json) == "22621":  # 判断是否请求频繁
                self.header = self.get_header()
            urls = self.parse_page_index(get_json)
            if urls:
                for url in urls:
                    url = start_url + url
                    html = self.get_page_detail(url)
                    if html:
                        to_mongodb = self.parse_page_detail(html, url, ID, Name)
                        # 写入数据库
                        if to_mongodb:
                            dt = to_mongodb['时间']
                            # 转换成时间数组
                            timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
                            # 转换成时间戳
                            timestamp = time.mktime(timeArray)
                            if timestamp < self.past_time:
                                return False
                            else:
                                table_name = ID + "_" + Name
                                self.save_to_mongodb(to_mongodb, str(table_name))
                return True

    """
        评论数据部分
    """
    # 获取页面信息
    def comments_get_page_index(self, comment_id, count=10, page=1):
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

    def parse_page_index_comment(self, html):
        try:
            data = json.loads(html)
            if data and 'comments' in data.keys():
                for item in data.get('comments'):
                    yield item
        except JSONDecodeError:
            pass

    # 采用正则表达式获取具体的信息
    def comments_parse_page_detail(self, html):
        datas = []
        items = self.parse_page_index_comment(html)
        for it in items:
            time = self.deal_data(str(it['created_at']))
            people = it['user']['screen_name']
            content = it['text']
            try:
                content_pattern = re.compile(r'<[^>]+>', re.S)
                content = content_pattern.sub('', content)
                content = content.replace("\u0026", "").replace("nbsp;", "").replace("\\u3000", "")
            except AttributeError:
                pass
            data = {
                '评论人': people,
                '评论时间': time,
                '评论内容': content
            }
            datas.append(data)
        return datas

    def get_comments(self, comment_id):
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

    def deal_url(self, url):
        if self.type == 3 or self.type == 4 or self.type == 5:
            comment_id = re.findall(re.compile('https://xueqiu.com/S/.*?/(\d+)', re.S), str(url))
        else:
            comment_id = re.findall(re.compile('https://xueqiu.com/\d+/(\d+)', re.S), str(url))
        return comment_id[0]

    def comments_from(self,db_name):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db = client[db_name]
        for table in db.collection_names():
            if table != 'system.indexes':
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        pinglun = item['评论']
                    except KeyError:
                        try:
                            url = item['url']
                            _id = item['_id']
                            print(url)
                            print("当前数据库为：", db_name, "\n 表为：", table)
                            comment_id = self.deal_url(url)
                            comments = self.get_comments(comment_id)
                            collection.update_one({"_id": _id}, {"$set": {"评论": comments}})
                        except KeyError:
                            print("url error")
                cursor.close()
    """
        评论内容结束
    """

    def main(self):
        print('总共数据表为:',self.collection_sjs.find().count())
        cursor = self.collection_sjs.find(no_cursor_timeout=True)
        for item in cursor:
            try:
                # A股代码
                ID = item['A股代码']
                # 公司简称
                Name = item['A股简称']
                start_url = XUEQIU_URL
                get_json = self.get_page_index(1, ID)
                if get_json:
                    max_page = self.get_maxPage(get_json)
                    for i in range(1, max_page + 1):
                        print("当前时间为：", datetime.datetime.now(), '当前表为：', ID+ '_'+ Name,'存储的数据库为：', self.databases, '总共页码：', max_page, '现在页码:', i)
                        if self.get_connent(ID, Name, i):
                            sleeptime = random.randint(1, 3)
                            time.sleep(sleeptime)
                        else:
                            break   # 跳出循环
            except Exception:
                print(item + "KeyError!!!")
        cursor.close()

        # 文章数据结束 开始评论数据爬取
        try:
            self.comments_from(self.databases)
        except Exception:
            self.comments_from(self.databases)

