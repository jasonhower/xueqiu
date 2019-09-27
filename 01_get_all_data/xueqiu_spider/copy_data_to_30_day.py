from xueqiu_spider.config import *
from multiprocessing import Pool
import time
import MySQLdb
import datetime
# 当天数据备份在30天数据库

import pymongo
class copy_middle_db(object):
    def __init__(self):
        self.pinglun_taolun_num  = 0
        self.pinglun_jiaoyi_num  = 0
        self.pinglun_xinwen_num  = 0
        self.pinglun_gonggao_num = 0
        self.pinglun_yanbao_num  = 0

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

    # 讨论内容拷贝
    def copy_taolun(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for table in db.collection_names():
            if table != 'system.indexes':
                print('taolun_copy,table name is ',table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
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
                        self.pinglun_taolun_num += 1
                        db_comment = client[MIDDLE_COMMENTS]
                        db_comment['taolun_comments'].insert(com)

                        sql = "INSERT INTO all_taolun_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(com['url'], com['股票名称'], com['评论人'], com['评论时间'], com['评论内容'])
                        print(sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print(e)
                cursor.close()

                # 删除表中所有元素
                collection.remove()

        self.close_MySQL(cur, conn)

    # 交易内容拷贝
    def copy_jiaoyi(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for table in db.collection_names():
            if table != 'system.indexes':
                print('jiaoyi_copy,table name is ',table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    print(item)
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
                        self.pinglun_jiaoyi_num += 1
                        db_comment = client[MIDDLE_COMMENTS]
                        db_comment['jiaoyi_comments'].insert(com)
                        sql = "INSERT INTO all_jiaoyi_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(com['url'], com['股票名称'], com['评论人'], com['评论时间'], com['评论内容'])
                        print(sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print(e)
                cursor.close()
                # 删除表中所有元素
                collection.remove()
        self.close_MySQL(cur, conn)

    # 新闻内容拷贝
    def copy_xinwen(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for table in db.collection_names():
            if table != 'system.indexes':
                print('xinwen_copy,table name is ',table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    print(item)
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
                        self.pinglun_xinwen_num += 1
                        db_comment = client[MIDDLE_COMMENTS]
                        db_comment['xinwen_comments'].insert(com)

                        sql = "INSERT INTO all_xinwen_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(com['url'], com['股票名称'], com['评论人'], com['评论时间'], com['评论内容'])
                        print(sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print(e)
                cursor.close()
                # 删除表中所有元素
                collection.remove()
        self.close_MySQL(cur, conn)


    # 公告内容拷贝
    def copy_gonggao(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for table in db.collection_names():
            if table != 'system.indexes':
                print('gonggao_copy,table name is ',table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    print(item)
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

                        self.pinglun_gonggao_num += 1

                        db_comment = client[MIDDLE_COMMENTS]
                        db_comment['gonggao_comments'].insert(com)

                        sql = "INSERT INTO all_gonggao_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(
                            com['url'], com['股票名称'], com['评论人'], com['评论时间'], com['评论内容'])
                        print(sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print(e)
                cursor.close()
                # 删除表中所有元素
                collection.remove()
        self.close_MySQL(cur, conn)

    # 研报内容拷贝
    def copy_yanbao(self, old_db, new_db):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db_new = client[new_db]
        db = client[old_db]

        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]

        for table in db.collection_names():
            if table != 'system.indexes':
                print('yanbao_copy,table name is ',table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    print(item)
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

                        self.pinglun_yanbao_num += 1

                        db_comment = client[MIDDLE_COMMENTS]
                        db_comment['yanbao_comments'].insert(com)

                        sql = "INSERT INTO all_yanbao_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(
                            com['url'], com['股票名称'], com['评论人'], com['评论时间'], com['评论内容'])
                        print(sql)
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except Exception as e:
                            print(e)
                cursor.close()
                # 删除表中所有元素
                collection.remove()
        self.close_MySQL(cur, conn)

    # 遍历5个数据库 将标记加 1
    def deal_db(self):
        self.mark(MIDDLE_TAOLUN_DB)
        self.mark(MIDDLE_JIAOYI_DB)
        self.mark(MIDDLE_XINWEN_DB)
        self.mark(MIDDLE_GONGGAO_DB)
        self.mark(MIDDLE_YANBAO_DB)

    def mark(self, DB):
        client = pymongo.MongoClient(MONGO_URL, connect=False)
        db = client[DB]
        for table in db.collection_names():
            if table != 'system.indexes':
                print('database is', DB, 'table name is ', table)
                collection = db[table]
                cursor = collection.find(no_cursor_timeout=True)
                for item in cursor:
                    try:
                        # 设置标记
                        count_key = item['count_key'] + 1
                        _id = item['_id']
                        collection.update_one({"_id": _id}, {"$set": {"count_key": count_key}})
                    except KeyError:
                        _id = item['_id']
                        collection.update_one({"_id": _id}, {"$set": {"count_key": 1}})
                cursor.close()


    def copy_middle_main(self):
        pool = Pool(5)
        pool.apply_async(self.copy_taolun, (TODAY_TAOLUN_DB, MIDDLE_TAOLUN_DB))
        time.sleep(1)
        pool.apply_async(self.copy_jiaoyi, (TODAY_JIAOYI_DB, MIDDLE_JIAOYI_DB))
        time.sleep(1)
        pool.apply_async(self.copy_xinwen, (TODAY_XINWEN_DB, MIDDLE_XINWEN_DB))
        time.sleep(1)
        pool.apply_async(self.copy_gonggao, (TODAY_GONGGAO_DB, MIDDLE_GONGGAO_DB))
        time.sleep(1)
        pool.apply_async(self.copy_yanbao, (TODAY_YANBAO_DB, MIDDLE_YANBAO_DB))
        time.sleep(1)
        pool.close()  # 关闭进程池
        pool.join()   # 主进程在这里等待，只有子进程全部结束之后，在会开启主线程
        # 标注信息加 1
        self.deal_db()
        # 将今天的评论数据个数插入到Mysql的everyday_comments_number表中
        myConn_list = self.start_MySQL()
        cur = myConn_list[1]
        conn = myConn_list[0]
        sql = "INSERT INTO everyday_comments_number(pinglun_date,taolun_num,jiaoyi_num,xinwen_num,gonggao_num,yanbao_num) VALUES('{}',{},{},{},{},{})".format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), int(self.pinglun_taolun_num), int(self.pinglun_jiaoyi_num), int(self.pinglun_xinwen_num), int(self.pinglun_gonggao_num), int(self.pinglun_yanbao_num))
        print(sql)
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
        self.close_MySQL(cur, conn)







