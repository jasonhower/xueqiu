from xueqiu_spider.config import *
from multiprocessing import Pool
import time

import pymongo

class copy_history_db(object):
    # 将超过最新记录的数据转移到历史数据里面
    def copy_taolun(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('taolun_copy,table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        if item['count_key'] > MIDDLE_TIME:
                            try:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': item['评论'],
                                }
                                print(data)
                            except KeyError:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': [],
                                }
                                print(data)
                                print("错误")
                            db_new[table].insert(data)
                            for da in data['评论']:
                                com = {
                                    'url': item['url'],
                                    '股票名称': item['股票名称'],
                                    '评论人': da['评论人'],
                                    '评论时间': da['评论时间'],
                                    '评论内容': da['评论内容']
                                }
                                db_comment = client[HISTORY_COMMENTS]
                                db_comment['yanbao_comments'].insert(com)
                    except KeyError:
                        print('count_key错误！' + str(item['_id']))
                cursor.close()

    # 交易内容拷贝
    def copy_jiaoyi(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('jiaoyi_copy,table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        if item['count_key'] > MIDDLE_TIME:
                            try:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': item['评论'],
                                }
                                print(data)
                            except KeyError:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': [],
                                }
                                print(data)
                                print("错误")
                            db_new[table].insert(data)
                            for da in data['评论']:
                                com = {
                                    'url': item['url'],
                                    '股票名称': item['股票名称'],
                                    '评论人': da['评论人'],
                                    '评论时间': da['评论时间'],
                                    '评论内容': da['评论内容']
                                }
                                db_comment = client[HISTORY_COMMENTS]
                                db_comment['yanbao_comments'].insert(com)
                    except KeyError:
                        print('count_key错误！' + str(item['_id']))
                cursor.close()

    # 新闻内容拷贝
    def copy_xinwen(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('xinwen_copy,table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        if item['count_key'] > MIDDLE_TIME:
                            try:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '摘要': item['摘要'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '外部链接': item['外部链接'],
                                    '评论': item['评论'],
                                }
                                print(data)
                            except KeyError:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '摘要': item['摘要'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '外部链接': item['外部链接'],
                                    '评论': [],
                                }
                                print(data)
                                print("错误")
                            db_new[table].insert(data)
                            for da in data['评论']:
                                com = {
                                    'url': item['url'],
                                    '股票名称': item['股票名称'],
                                    '评论人': da['评论人'],
                                    '评论时间': da['评论时间'],
                                    '评论内容': da['评论内容']
                                }
                                db_comment = client[HISTORY_COMMENTS]
                                db_comment['yanbao_comments'].insert(com)
                    except KeyError:
                        print('count_key错误！' + str(item['_id']))
                cursor.close()

    # 公告内容拷贝
    def copy_gonggao(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('gonggao_copy,table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        if item['count_key'] > MIDDLE_TIME:
                            try:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    'PDF下载链接': item['PDF下载链接'],
                                    '评论': item['评论'],
                                }
                                print(data)
                            except KeyError:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    'PDF下载链接': item['PDF下载链接'],
                                    '评论': [],
                                }
                                print(data)
                                print("错误")
                            db_new[table].insert(data)
                            for da in data['评论']:
                                com = {
                                    'url': item['url'],
                                    '股票名称': item['股票名称'],
                                    '评论人': da['评论人'],
                                    '评论时间': da['评论时间'],
                                    '评论内容': da['评论内容']
                                }
                                db_comment = client[HISTORY_COMMENTS]
                                db_comment['yanbao_comments'].insert(com)
                    except KeyError:
                        print('count_key错误！' + str(item['_id']))
                cursor.close()

    # 研报内容拷贝
    def copy_yanbao(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('gonggao_copy,table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        if item['count_key'] > MIDDLE_TIME:
                            try:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': item['评论'],
                                }
                                print(data)
                            except KeyError:
                                data = {
                                    'url': item['url'],
                                    'A股代码': item['A股代码'],
                                    '股票名称': item['股票名称'],
                                    '时间': item['时间'],
                                    '发布者': item['发布者'],
                                    '标题': item['标题'],
                                    '正文': item['正文'],
                                    '转发量': item['转发量'],
                                    '评论量': item['评论量'],
                                    '点赞量': item['点赞量'],
                                    '评论': [],
                                }
                                print(data)
                                print("错误")
                            db_new[table].insert(data)
                            for da in data['评论']:
                                com = {
                                    'url': item['url'],
                                    '股票名称': item['股票名称'],
                                    '评论人': da['评论人'],
                                    '评论时间': da['评论时间'],
                                    '评论内容': da['评论内容']
                                }
                                db_comment = client[HISTORY_COMMENTS]
                                db_comment['yanbao_comments'].insert(com)
                    except KeyError:
                        print('count_key错误！' + str(item['_id']))
                cursor.close()

    def copy_history_main(self):
        self.copy_taolun(MIDDLE_TAOLUN_DB, HISTORY_TAOLUN_DB)
        self.copy_jiaoyi(MIDDLE_JIAOYI_DB, HISTORY_JIAOYI_DB)
        self.copy_xinwen(MIDDLE_XINWEN_DB, HISTORY_XINWEN_DB)
        self.copy_gonggao(MIDDLE_GONGGAO_DB, HISTORY_GONGGAO_DB)
        self.copy_yanbao(MIDDLE_YANBAO_DB, HISTORY_YANBAO_DB)




