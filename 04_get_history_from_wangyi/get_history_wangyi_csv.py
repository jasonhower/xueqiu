import requests
import time
from lxml import etree
import pymongo
import re
class Download_HistoryStock(object):
    def __init__(self, code, name, totalCount):
        self.code = code
        self.name = name
        self.totalCount = totalCount
        self.start_url = "http://quotes.money.163.com/trade/lsjysj_" + totalCount + ".html#01b07"
        print(self.start_url)
        self.headers = {
            "User-Agent": ":Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
        }

    def parse_url(self):
        response = requests.get(self.start_url)
        print(response.status_code)
        if response.status_code == 200:
            return etree.HTML(response.content)
        return False

    def get_date(self, response):
        # 得到开始和结束的日期
        start_date = ''.join(response.xpath('//input[@name="date_start_type"]/@value')[0].split('-'))
        end_date = ''.join(response.xpath('//input[@name="date_end_type"]/@value')[0].split('-'))
        return start_date, end_date

    def download(self, start_date, end_date):
        download_url = "http://quotes.money.163.com/service/chddata.html?code=" + self.code + "&start=" + start_date + "&end=" + end_date + "&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP"
        print(download_url)
        data = requests.get(download_url)
        f = open('D://history//' + self.name + '.csv', 'wb')

        for chunk in data.iter_content(chunk_size=10000):
            if chunk:
                f.write(chunk)
        print('股票---', self.code, '历史数据正在下载')

    def run(self):
        try:
            html = self.parse_url()
            start_date, end_date = self.get_date(html)
            self.download(start_date, end_date)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    # 获取股票列表
    client = pymongo.MongoClient('192.168.1.108', connect=False)
    db = client['company']
    for table in db.collection_names():
        if table != 'system.indexes':
            print('table name is ', table)
            collection = db[table]
            cursor = collection.find(no_cursor_timeout=True)
            for item in cursor:
                name = item['A股代码']
                totalCount = re.sub("\D", "", name)
                if table =='shanghai':
                    temp_code = '0' + totalCount
                elif table == 'shenzhen':
                    temp_code = '1' + totalCount
                print(temp_code)
                time.sleep(1)
                download = Download_HistoryStock(temp_code, item['A股代码'] + '_' + item['A股简称'], totalCount)
                download.run()
            cursor.close()
