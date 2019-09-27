from xueqiu_spider.content_spider import *
from xueqiu_spider.copy_data_to_30_day import *
from xueqiu_spider.copy_data_to_history import *
from multiprocessing import Pool
import time


def run_main(type, divide_db, divide_collection, add_time):
    # type, divide_db, divide_collection, add_time
    dealhtml = DealHtml(type, divide_db, divide_collection, add_time)
    dealhtml.main()


if __name__ == '__main__':
    # 实现20个进程池
    pool = Pool(20)

    for i in range(6):
        pool.apply_async(run_main, (i, COME_DB, SHANGHAI_A, PAST_DAY))
        time.sleep(1)

    for i in range(6):
        pool.apply_async(run_main, (i, COME_DB, SHENZHEN_A, PAST_DAY))
        time.sleep(1)

    pool.close()  # 关闭进程池
    pool.join()   # 主进程在这里等待，只有子进程全部结束之后，在会开启主线程

    # 24小时 --> 30天数据
    cp = copy_middle_db()
    cp.copy_middle_main()

    # 30天  --> 历史数据
    cp = copy_history_db()
    cp.copy_history_main()

