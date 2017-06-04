# -*- coding: utf-8 -*-
"""

"""
import time
import datetime
import requests
from requests import exceptions
import configparser
import redis
import sys
import MySQLdb as mdb
import json
import threading
from user_info import UserInfo
from bs4 import BeautifulSoup


class ZhihuSprider(threading.Thread):

    reload(sys)
    sys.setdefaultencoding('utf-8')

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        "Host": "www.zhihu.com",
        "authorization": "Bearer Mi4wQUJDTVBoUG1sQWdBa0VMY3ZnZW5DeGNBQUFCaEFsVk5FcXdsV1FBenJLRUNiZlA2clVNWkZwNUNKTUN4cDJObUlR|1493049110|0b9a452b773b5e2146b2bf587bbee5dc2c6abdbe",
        "Referer": "https://www.zhihu.com",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
    }

    def __init__(self, threadID=1, threadName=''):

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.threadName = threadName
        self.threadLock = threading.Lock()

        self.cookies = {
           }

        self.headers = ZhihuSprider.headers
        self.base_url = "https://www.zhihu.com"
        self.session = requests.session()

        self.start_user = 'miloyip'     # 爬虫起始用户
        self.start_url_token = 'Mr_why'  # 起始用户的 url_token
        # 用户关注列表的url
        self.followee_url = self.base_url + '/api/v4/members/{user}/followees?include={include}&offset={offset}&limit={limit}'
        # 用户关注列表url的请求参数
        self.followee_param = 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics'
        # 用户粉丝列表的url
        self.follower_url = self.base_url + '/api/v4/members/{user}/followers?include={include}&offset={offset}&limit={limit}'
        # 用户粉丝列表url的请求参数
        self.follower_param = 'data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics'
        # 用户详细资料的url
        self.user_url = self.base_url + '/api/v4/members/{user}?include={include}'
        # 用户详细资料url的请求参数
        self.user_param = 'locations,employments,gender,educations,business,voteup_count,thanked_Count,follower_count,following_count,cover_url,following_topic_count,following_question_count,following_favlists_count,following_columns_count,avatar_hue,answer_count,articles_count,pins_count,question_count,commercial_question_count,favorite_count,favorited_count,logs_count,marked_answers_count,marked_answers_text,message_thread_token,account_status,is_active,is_force_renamed,is_bind_sina,sina_weibo_url,sina_weibo_name,show_sina_weibo,is_blocking,is_blocked,is_following,is_followed,mutual_followees_count,vote_to_count,vote_from_count,thank_to_count,thank_from_count,thanked_count,description,hosted_live_count,participated_live_count,allow_message,industry_category,org_name,org_homepage,badge[?(type=best_answerer)].topics'

        # 获取配置
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        # 初始化redis连接
        try:
            redis_host = self.config.get('redis', 'host')
            redis_port = self.config.get('redis', 'port')
            self.redis_con = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
            self.redis_con.lpush('redis_db_user', 'yunhua_lee')
            self.redis_con.lpush('redis_db_user', 'zenzen')
            self.redis_con.lpush('redis_db_user', 'markzhai')
            self.redis_con.lpush('redis_db_user', 'jackfeng')
            print 'redis 连接成功'.decode('utf-8').encode('gbk')
        except:
            print("请安装redis或检查redis连接配置").decode('utf-8').encode('gbk')
            sys.exit()

        # 初始化数据库连接
        try:
            db_host = self.config.get('db', 'host')
            db_port = int(self.config.get("db", "port"))
            db_user = self.config.get("db", "user")
            db_pass = self.config.get("db", "password")
            db_db = self.config.get("db", "db")
            db_charset = self.config.get("db", "charset")
            self.db = mdb.connect(host=db_host, port=db_port, user=db_user, 
                                  passwd=db_pass, db=db_db, charset=db_charset)
            self.db_cursor = self.db.cursor()
            print '数据库连接成功'.decode('utf-8').encode('gbk')
        except:
            print("请检查数据库配置").decode('utf-8').encode('gbk')
            sys.exit()

    def get_xsrf(self):
        response = self.session.get(self.base_url, headers=self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        xsrf = soup.find('input', attrs={"name": "_xsrf"}).get("value")
        return xsrf

    def get_captcha(self):
        """
            把验证码图片保存到当前目录，手动识别验证码
            :return:
        """
        # time.time 返回的时间是10位，需要扩展至13位
        t = str(int(time.time() * 1000))
        captcha_url = self.base_url + '/captcha.gif?r=' + t + "&type=login"
        r = self.session.get(captcha_url, headers=self.headers)
        with open('captcha.jpg', 'wb') as f:
            f.write(r.content)
        captcha = raw_input("验证码：".decode('utf-8').encode('gbk'))
        return captcha

    def login(self, phone_num, password):
        login_url = self.base_url + '/login/phone_num'
        data = {
            'phone_num': phone_num,
            'password': password,
            '_xsrf': self.get_xsrf(),
            "captcha": self.get_captcha(),
        }
        response = self.session.post(login_url, data=data, headers=self.headers)
        # login_code = response.json()
        # print login_code['msg']
        return response

    # 爬取关注列表
    def followee_request(self, url):
        try:
            time.sleep(2)
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            return response.content
        except exceptions.ConnectionError:
            time.sleep(1)
            print '尝试重新连接...'.decode('utf-8').encode('gbk')
            self.followee_request(url)

    # 爬取粉丝列表
    def follower_request(self, url):
        try:
            time.sleep(2)
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            return response.content
        except exceptions.ConnectionError:
            time.sleep(1)
            print '尝试重新连接...'.decode('utf-8').encode('gbk')
            self.follower_request(url)

    # 爬取用户个人详细信息
    def get_user_data(self, url):
        try:
            time.sleep(1)
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            return response.content
        except exceptions.ConnectionError:
            time.sleep(1)
            print '尝试重新连接...'.decode('utf-8').encode('gbk')
            self.get_user_data(url)

    # 分析用户信息
    def analyze_user(self, user_url, followee_url, follower_url):
        user_info = UserInfo()
        result = json.loads(self.get_user_data(user_url))
        # 需要的用户信息
        user_info.name = result.get('name')
        user_info.url_token = result.get('url_token')

        if 'locations' in result.keys():
            locations = result.get('locations')
            if len(locations) > 0:
                user_info.location = locations[0].get('name')
            else:
                user_info.location = ''
        else:
            user_info.location = ''

        user_info.gender = result.get('gender')

        if 'employments' in result.keys():
            employments = result.get('employments')
            if len(employments) > 0:
                if 'company' in employments[0].keys():
                    company = employments[0].get('company')
                    user_info.company = company.get('name')
                else:
                    user_info.company = ''
                if 'job' in employments[0].keys():
                    job = employments[0].get('job')
                    user_info.job = job.get('name')
                else:
                    user_info.job = ''
            else:
                user_info.company = ''
                user_info.job = ''
        else:
            user_info.company = ''
            user_info.job = ''

        if 'educations' in result.keys():
            educations = result.get('educations')
            if len(educations) > 0:
                if 'school' in educations[0].keys():
                    school = educations[0].get('school')
                    user_info.school = school.get('name')
                else:
                    user_info.school = ''
                if 'major' in educations[0].keys():
                    major = educations[0].get('major')
                    user_info.major = major.get('name')
                else:
                    user_info.major = ''
            else:
                user_info.school = ''
                user_info.major = ''
        else:
            user_info.school = ''
            user_info.major = ''

        user_info.answer_count = result.get('answer_count')
        user_info.articles_count = result.get('articles_count')

        if 'business' in result.keys():
            business = result.get('business')
            user_info.business = business.get('name')
        else:
            user_info.business = ''

        user_info.follower_count = result.get('follower_count')
        user_info.following_count = result.get('following_count')
        user_info.headline = result.get('headline')
        user_info.participated_live_count = result.get('participated_live_count')
        user_info.question_count = result.get('question_count')
        user_info.thanked_count = result.get('thanked_count')
        user_info.voteup_count = result.get('voteup_count')

        global cnt
        self.threadLock.acquire()
        print cnt
        print 'url_token: %s' % user_info.url_token
        cnt = cnt + 1
        self.threadLock.release()
        replace_data = (user_info.url_token,user_info.name,user_info.gender,user_info.follower_count,
                        user_info.following_count,user_info.voteup_count,user_info.thanked_count,
                        user_info.participated_live_count,user_info.business,user_info.company,
                        user_info.school,user_info.major,user_info.job,user_info.location,
                        user_info.question_count,user_info.answer_count,user_info.articles_count,user_info.headline)

        replace_sql = '''REPLACE INTO zhihu_user(url_token,username,gender,follower_count,
                          following_count, voteup_count,thanked_count,participated_live_count,business,
                          company,school,major,
                          job,location,question_count,answer_count,articles_count,headline)
                          VALUES(%s,%s,%s,%s,
                          %s,%s,%s,%s,%s,%s,%s,%s,
                          %s,%s,%s,%s,%s,%s)'''
        self.db_cursor.execute(replace_sql, replace_data)
        self.db.commit()

        # 获取关注者的url_token信息
        followee_result = json.loads(self.followee_request(followee_url))
        self.store_url_token(followee_result)
        if 'paging' in followee_result.keys() and followee_result.get('paging').get('is_end') == False:
            next_page_url = followee_result.get('paging').get('next')
            followee_rst = json.loads(self.followee_request(next_page_url))
            self.store_url_token(followee_rst)

        # # 获取粉丝的url_token信息
        # follower_result = json.loads(self.follower_request(follower_url))
        # self.store_url_token(followee_result)
        # if 'paging' in follower_result.keys() and follower_result.get('paging').get('is_end') == False:
        #     next_page_url = follower_result.get('paging').get('next')
        #     follower_rst = json.loads(self.follower_request(next_page_url))
        #     self.store_url_token(follower_rst)

    # 将 url_token 存入 redis
    def store_url_token(self, followee_result):
        user_list = followee_result.get('data')
        for user in user_list:
            # 获取用户的url_token
            url_token = user.get('url_token')
            # url_token 存入 redis
            # sadd 方法的返回值是被添加到集合中的新元素的数量，不包括被忽略的元素
            # 判断是否已抓取
            self.threadLock.acquire()
            if self.redis_con.sadd('redis_has_db', url_token):
                self.redis_con.lpush('redis_db_user', url_token)
            self.threadLock.release()

    def bfs_search(self):
        while True:
            tmp = self.redis_con.rpop('redis_db_user')
            if type(tmp) is None:
                print 'empty'
                break
            user_url = self.user_url.format(user=tmp, include=self.user_param)
            followee_url = self.followee_url.format(user=tmp, include=self.followee_param, offset=0, limit=20)
            follower_url = self.follower_url.format(user=tmp, include=self.follower_param, offset=0, limit=20)
            self.analyze_user(user_url, followee_url, follower_url)

    def run(self):
        print(self.threadName + " is running")
        # self.redis_con.lpush('redis_db_user', self.start_url_token)
        self.bfs_search()
# ------------------------------------------------------------------------------

cnt = 1
threads = []
threads_num = 3
for i in range(0, threads_num):
    m = ZhihuSprider(i, "thread" + str(i))
    threads.append(m)

for i in range(0, threads_num):
    threads[i].start()

for i in range(0, threads_num):
    threads[i].join()
