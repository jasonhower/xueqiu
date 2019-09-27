# Mongodb 的地址
MONGO_URL = '192.168.1.108'

# 雪球网地址
XUEQIU_URL = 'https://xueqiu.com'

# ip代理池地址
PROXY_POOL_URL = 'http://127.0.0.1:5000/get'

# A股来源数据库
COME_DB = 'company'

# A股来源表
SHENZHEN_ZB = 'shanghai_1'    # 深圳主板数据
SHENZHEN_ZXQY = 'shanghai_2'  # 深圳中小企业板数据
SHENZHEN_CYB = 'shanghai_3'   # 深圳创业板数据
SHANGHAI_A = 'shanghai_4'     # 上海A股数据

SHANGHAI_A = 'shanghai'     # 上海A股数据
SHENZHEN_A = 'shenzhen'     # 深圳A股数据

# 间隔时间(天、时、分、秒)(每天爬取前一天数据)
PAST_DAY = 1 * 24 * 60 * 60


# 保存的数据库地址
TODAY_TAOLUN_DB = 'day1_taolun'
TODAY_JIAOYI_DB = 'day1_jiaoyi'
TODAY_XINWEN_DB = 'day1_xinwen'
TODAY_GONGGAO_DB = 'day1_gonggao'
TODAY_YANBAO_DB = 'day1_yanbao'


# 30天临时数据库
MIDDLE_TIME = 30

MIDDLE_TAOLUN_DB = 'day30_taolun'
MIDDLE_JIAOYI_DB = 'day30_jiaoyi'
MIDDLE_XINWEN_DB = 'day30_xinwen'
MIDDLE_YANBAO_DB = 'day30_yanbao'
MIDDLE_GONGGAO_DB = 'day30_gonggao'

MIDDLE_COMMENTS = 'middle_comments'

# 历史数据库
HISTORY_TAOLUN_DB = 'xueqiu_history_taolun'
HISTORY_JIAOYI_DB = 'xueqiu_history_jiaoyi'
HISTORY_XINWEN_DB = 'xueqiu_history_xinwen'
HISTORY_YANBAO_DB = 'xueqiu_history_yanbao'
HISTORY_GONGGAO_DB = 'xueqiu_history_gonggao'

HISTORY_COMMENTS = 'xueqiu_history_comments'

TAOLUN_DB_MYSQL = 'all_taolun_comments_2'
JIAOYI_DB_MYSQL = 'all_jiaoyi_comments_2'
XINWEN_DB_MYSQL = 'all_xinwen_comments_2'
YANBAO_DB_MYSQL = 'all_yanbao_comments_2'
GONGGAO_DB_MYSQL = 'all_gonggao_comments_2'


