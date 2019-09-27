该项目主要是以雪球网中股票讨论、交易、咨询、公告和研报中用户评论数据的爬取。

* 01_get_all_data：(主要)爬取前一天数据
* 02_add_new_comments：(主要)用户评论数据定时更新
* 03_get_cookies：模拟自动登录获取Cookies
* 04_get_history_from_wangyi：定期从网易财经上爬取股票历史交易数据
* 05_MongoDB_to_MySQL：将MongoDB数据导入到MySQL中

![雪球网爬取流程]()