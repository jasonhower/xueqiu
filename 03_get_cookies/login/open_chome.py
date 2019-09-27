# 模拟自动登录
import time
import random
from login.chome_config import *
from login.save_cookie import *
from ym.ym import ym
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options

options = webdriver.ChromeOptions()

options.add_argument('--headless')
options.add_argument('--disable-gpu')


# 设置中文
options.add_argument('lang=zh_CN.UTF-8')
uaList = USER_AGENT



agent = random.choice(uaList)
print(agent)
options.binary_location = r'C:\Users\wangk\AppData\Local\Google\Chrome\Application\chrome.exe'
# options.binary_location = '/opt/google/chrome/chrome'
# 更换头部
options.add_argument(
    'user-agent="{}"'.format(agent))


# chrome_options = Options()
options.add_argument('window-size=1920x3000') #指定浏览器分辨率
options.add_argument('--disable-extensions')
options.add_argument('--disable-gpu') #谷歌文档提到需要加上这个属性来规避bug
# chrome_options.add_argument('--hide-scrollbars') #隐藏滚动条, 应对一些特殊页面
# chrome_options.add_argument('blink-settings=imagesEnabled=false') #不加载图片, 提升速度
options.add_argument('--no-sandbox')
options.add_argument('--headless') #浏览器不提供可视化页面. linux下如果系统不支持可视化不加这条会启动失败
# chrome_options.binary_location = r'/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary' #手动指定使用的浏览器位置
options.binary_location = r'/opt/google/chrome/chrome'

# opener = webdriver.Chrome(r'/usr/local/bin/chromedriver', chrome_options=chrome_options)


browser = webdriver.Chrome(r'/usr/local/bin/chromedriver', chrome_options=options)
wait = WebDriverWait(browser, 10)

y = ym()


def open_chrome():
    try:
        browser.get('https://xueqiu.com/')
        print(browser.get_cookies())
        time.sleep(2)
        browser.find_element_by_class_name('nav__login__regist').click()
        # 获取手机号
        phone = y.get_mobile()
        print(phone)
        browser.find_element_by_css_selector('#app > div.modals.dimmer.js-shown > div:nth-child(1) > div.modal.modal__login > div.modal__login__main > div.modal__login__mod > div.modal__login__regist > div.modal__login__form > form > div:nth-child(1) > input[type="text"]').send_keys(phone)
        browser.find_element_by_css_selector('#app > div.modals.dimmer.js-shown > div:nth-child(1) > div.modal.modal__login > div.modal__login__main > div.modal__login__mod > div.modal__login__regist > div.modal__login__form > form > div:nth-child(1) > span:nth-child(3) > a').click()

        code = y.get_sms(phone)
        print(code)
        browser.find_element_by_css_selector('#app > div.modals.dimmer.js-shown > div:nth-child(1) > div.modal.modal__login > div.modal__login__main > div.modal__login__mod > div.modal__login__regist > div.modal__login__form > form > div:nth-child(2) > input[type="text"]').send_keys(code)
        time.sleep(2)
        browser.find_element_by_css_selector('#app > div.modals.dimmer.js-shown > div:nth-child(1) > div.modal.modal__login > div.modal__login__main > div.modal__login__btn').click()
        time.sleep(1)
        print(browser.get_cookies())
        time.sleep(4)
        browser.find_element_by_css_selector('#app > div.modals.dimmer.js-shown > div:nth-child(2) > div > div.modal__confirm__btns > a.button.button-lg.modal__confirm__submit').click()
        time.sleep(4)
        cookies = browser.get_cookies()
        print(cookies)
        cook = ""
        for cookie in browser.get_cookies():
            cook = cook + cookie['name'] + '=' + cookie['value'] + ';'
            print(cookie['name'], cookie['value'])
        print(cook)
        if save_cookie(cook,agent):
            print("Save to Mongodb Successfully")
            browser.close()
            return True
    except Exception:
        open_chrome()

