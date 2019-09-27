from add_comments.config import *
from add_comments.new_comments_spider import *
from multiprocessing import Pool
import time


def run_main(type, databases):
    com = add_comments(type, databases)
    com.main()

if __name__ == '__main__':
    # 实现5个进程池
    pool = Pool(5)

    pool.apply_async(run_main, (1, MIDDLE_TAOLUN_DB))
    time.sleep(2)
    pool.apply_async(run_main, (2, MIDDLE_JIAOYI_DB))
    time.sleep(2)
    pool.apply_async(run_main, (3, MIDDLE_XINWEN_DB))
    time.sleep(2)
    pool.apply_async(run_main, (4, MIDDLE_GONGGAO_DB))
    time.sleep(2)
    pool.apply_async(run_main, (5, [MIDDLE_YANBAO_DB]))

    pool.close()  # 关闭进程池
    pool.join()   # 主进程在这里等待，只有子进程全部结束之后，在会开启主线程
