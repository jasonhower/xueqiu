#coding=utf-8
import pymongo
import MySQLdb


def start_MySQL():
    conn = MySQLdb.connect(
            host='192.168.1.108',
            port=3306,
            user='',
            passwd='',
            db='xueqiu',
            charset='utf8')

    cur = conn.cursor()
    myConn_list = [conn, cur]
    return myConn_list


def close_MySQL(cur,conn):
    cur.close()
    conn.close()


if __name__ == "__main__":
    client = pymongo.MongoClient('192.168.1.108', 27017)
    TempleSpider = client['all_comments']
    temple_comment_collect = TempleSpider['all_xinwen_comments']

    myConn_list = start_MySQL()
    cur = myConn_list[1]
    conn = myConn_list[0]

    for temple in temple_comment_collect.find():
        sql = "INSERT INTO all_xinwen_comments_2(url,company_name,comment_people,comment_time,comment_content) VALUES ('{}', '{}', '{}', '{}', '{}')".format(temple['url'],temple['股票名称'],temple['评论人'],temple['评论时间'],temple['评论内容'])
        print(sql)
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
                sql = "insert into error(url,come_from) VALUES('{}','xinwen')".format(temple['url'])
                print(e)
                print(sql + "错误")
                cur.execute(sql)
                conn.commit()
    close_MySQL(cur, conn)