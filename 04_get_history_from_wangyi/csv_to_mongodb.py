
import csv
import pymongo

import os.path
import re


# 遍历指定目录，显示目录下的所有文件名
def eachFile(filepath):
    pathDir = os.listdir(filepath)
    print(pathDir)
    for allDir in pathDir:
        child = os.path.join('%s\%s' % (filepath, allDir))
        if os.path.isfile(child):
            with open(child, 'rt', encoding="ANSI") as csvfile:
                reader = csv.DictReader(csvfile)
                column = [row for row in reader]
            print(column)
            client = pymongo.MongoClient('192.168.1.108', connect=False)
            db = client['all_history_transaction_data']
            db[allDir[:-4]].insert(column)

if __name__ == '__main__':
    # get_csv()
    filenames = 'D:\history'  # refer root dir
    arr = []
    eachFile(filenames)