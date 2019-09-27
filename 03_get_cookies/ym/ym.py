#  -*- coding: utf-8 -*-
# 易码平台对接
from ym.ym_config import *
import requests
import re
import time
class ym():
    def __init__(self):
        self.token = TOKEN
        self.itemid = ITEMID

    # 获取账户信息
    def get_accountinfo(self):
        url = 'http://api.fxhyd.cn/UserInterface.aspx?action=getaccountinfo&token=' + self.token
        html = self.get_html(url)
        return self.deal_html(html)

    # 获取电话号码
    def get_mobile(self):
        url = 'http://api.fxhyd.cn/UserInterface.aspx?action=getmobile&itemid={}&token={}'.format(self.itemid, self.token)
        print(url)
        html = self.get_html(url)
        re = self.deal_html(html)
        if re[0] == 'success':
            return re[1]

    # 释放电话号码
    def release(self, mobile):
        url = 'http://api.fxhyd.cn/UserInterface.aspx?action=release&mobile={}&itemid={}&token={}'.format(mobile, self.itemid, self.token)
        html = self.get_html(url)
        return self.deal_html(html)

    # 释放全部电话号码
    def release_all(self):
        url = 'http://api.fxhyd.cn/UserInterface.aspx?action=releaseall&token={}'.format(self.token)
        html = self.get_html(url)
        return self.deal_html(html)

    # 获取短信
    def get_sms(self, mobile):
        for n in range(1, 13):
            time.sleep(5)
            url = 'http://api.fxhyd.cn/UserInterface.aspx?action=getsms&mobile={}&itemid={}&token={}&release=1'.format(mobile, self.itemid, self.token)
            html = self.get_html(url)
            print(html)
            if html != '3001':
                res = self.deal_html(html)
                print(res)
                if res[0] == 'success':
                    info = re.findall(r'\d+', res[1])
                    return info[0]
        return None

    # 获取短信发送状态
    def get_send_sms_state(self):
        pass

    # 获取网页信息
    def get_html(self, url):
        try:
            response = requests.get(url)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                return response.text
            return None
        except ConnectionError:
            return None

    # 解析网页信息
    def deal_html(self, html):
        print(html)
        return html.split("|")


