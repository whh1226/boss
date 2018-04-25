#请求模块
import requests
#json模块
import json
#时间模块
import time
import pymysql
import threading
import _thread
import queue
from lxml import etree
from SuperCode import dcVerCode

'''
代码只爬取了python相关全国的招聘信息，https://www.zhipin.com/common/data/position.json  
这个接口里边是带有所有职位的关键字段，如有需要，与城市信息的提取方式类似
由于数据量较大，可以考虑用多线程/多进程/分布式来加快爬取速度，也可以使用框架
'''

class Boss():
    def __init__(self):
        self.url = 'https://www.zhipin.com/common/data/city.json'#获取地区id的url
        self.base_url = 'https://www.zhipin.com'#初始url
        self.headers = {
                    'cookie': 't=pPsSyl1rGhMHNrgs; wt=pPsSyl1rGhMHNrgs; JSESSIONID=""; __c=1523589131; __g=-;'
                   ' __l=l=%2Fwww.zhipin.com%2F&r=https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DscHbokjqF7nK8kca00Pxriw_'
                   '9kWr_EE5r9a1c7Os_7sblhL_v2u9hhM410bRniDt%26wd%3D%26eqid%3D9587f0880001bc6c000000025ad02003; Hm_lvt_'
                   '194df3105ad7148dcf2b98a91b5e727a=1523589131,1523589686,1523590023,1523590172; lastCity=101050100; '
                   'Hm_lpvt_194df3105ad7148dcf2b98a91b5e727a=1523594105; __a=99452609.1521718391.1523272826.1523589131.94.13.43.94',
                    'referer': 'https://www.zhipin.com/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/65.0.3325.181 Safari/537.36',
                    'x-requested-with':'XMLHttpRequest',
                    }#模拟浏览器请求报头信息
        self.proxy = {'http': 'http://219.239.236.208:4333',
                      'https': 'https://219.239.236.209:4333'}#代理IP池，防止请求频繁遭到封禁
        # self.proxy = {'http': 'http://H01T3Z8ZSM11D61D:154F545DD00DA6B3@proxy.abuyun.com:9020',
        #               'https': 'http://H01T3Z8ZSM11D61D:154F545DD00DA6B3@proxy.abuyun.com:9020'
        #               }
        self.item = {}#用于接收数据的空字典

    def auth_code(self,html):
        '''
        下载验证码图片后，利用打码平台识别验证码，将验证码提交后重新开始爬取
        :param html: 
        :return: 
        '''
        code_pic = html.xpath('//p/img/@src')[0]
        code_pic_url = self.base_url+code_pic
        response = requests.get(code_pic_url,headers=self.headers,proxies=self.proxy).content
        with open('code.png', 'wb') as f:#将图片下载至本地
            f.write(response)
        sub_url = self.base_url + html.xpath('//form/@action')[0]  # 提交验证的链接
        randomKey = html.xpath('//input/@value')[0]#提交需求的参数
        client = dcVerCode('jtfc123','liu5706498','0')#打码平台账号密码
        yzm, imageId = client.recYZM("code.png")#利用打码平台获取验证码图片中的文本内容
        data = {
            'randomKey': randomKey,
            'captcha': yzm
        }  # 提交验证码的参数
        sub_response = requests.post(sub_url, headers=self.headers, data=data)

    def get_city(self):#获取各个城市的招聘信息链接
        response = requests.get(self.url,headers=self.headers,proxies=self.proxy).text
        city_result = json.loads(response)#将抓取到的json数据进行转换
        city_info_list = []#用于接收包含城市id和名称的列表格式为[('101050100','哈尔滨'),(),....]
        for i in city_result['data']['cityList']:
            for c in i['subLevelModelList']:
                city_info = c['code'],c['name']
                city_info_list.append(city_info)
        self.get_page(city_info_list)

    def get_page(self,city_info_list,page=1):#获取每一页的源码,接收一个page参数用于翻页，默认为1
        city_url = 'https://www.zhipin.com/c{}/h_{}/?query=Python&page={}'.format(city_info_list[0][0],city_info_list[0][0],page)
        response = requests.get(city_url,headers=self.headers,proxies=self.proxy).text
        html = etree.HTML(response)
        self.item['city'] = city_info_list[0][1]
        try:
            is_max = html.xpath('//a[@ka="page-next"]/@class')[0]#通过判断is_max的值来确定是否有下一页招聘信息
            if city_info_list == []:
                print('抓取完毕')
            if is_max == 'next':
                '''
                如果本地区还有下一页招聘信息，则将page+1，继续调用翻页函数翻页
                '''
                print('正在爬取{}地区第{}页的招聘信息...'.format(self.item['city'], page))
                page+=1
                self.get_position_url(html)
                self.get_page(city_info_list,page)
                time.sleep(3)#歇一会
            elif is_max == 'next disabled':
                '''
                如果本地区没有下一页招聘信息，则将地区的id以及名称从列表中删除，并且再次调用函数，进行其他地区翻页
                '''
                print('正在爬取{}地区第{}页的招聘信息...'.format(self.item['city'], page))
                del city_info_list[0]
                self.get_position_url(html)
                self.get_page(city_info_list)
                time.sleep(3)#歇一会
        except Exception as e:
            c_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            with open('error.log','a+') as f:
                f.write('Error:{}--time:{}'.format(e,c_time)+'\n')
            print('出现验证码，正在准备处理.............')
            self.auth_code(html)
            print('验证码提交成功,正在准备重新爬取')
            self.get_city()

    def get_position_url(self,html):#获取每一个职位的链接
        '''
        在这里使用了增量爬虫，在进行请求以前，获取数据库中已经爬取得职位链接，如果已经存在，则不爬取
        '''
        url_list = html.xpath('//div[@class="info-primary"]/h3/a/@href')
        for url in url_list:
            position_url = self.base_url+url
            cursor.execute('select position_url from boss')
            old_url = cursor.fetchall()
            old_url_list = []
            '''
            由于使用fetchall获取到的数据是(('',),('',),('',))这样的形式，无法进行对比，因此需要转换为列表
            '''
            result = (j for i in old_url for j in i)
            for old in result:
                old_url_list.append(old)
            # print(old_url_list)
            if position_url not in old_url_list:#如果爬取到的链接不在数据库中，则进行爬取
                self.parse_position(position_url)

    def parse_position(self,url):#解析职位链接，获取公司，职位以及工作地址信息
        response = requests.get(url,headers=self.headers,proxies=self.proxy).text
        html = etree.HTML(response)
        try:
            self.item['position_url'] = url
            self.item['company'] = html.xpath('//a[@ka="job-detail-company"]/text()')[0]
            self.item['position'] = html.xpath('//div[@class="name"]/h1/text()')[0]
            self.item['address'] = html.xpath('//div[@class="location-address"]/text()')[0]
            # print(position,company,address)
            time.sleep(3)#歇一会
            self.save()
        except Exception as e:
            c_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            with open('error.log','a+') as f:
                f.write('Error:{}--time:{}'.format(e,c_time)+'\n')
            print('出现验证码，正在准备处理.............')
            self.auth_code(html)
            print('验证码提交成功,正在准备重新爬取')
            self.get_city()

    def save(self):#入库
        '''  
        如果表不存在，则创建表，否则跳过
        '''
        cursor.execute('use Boss')
        cursor.execute('create table if not EXISTS boss(company VARCHAR(255),position VARCHAR(255),'
                       'address VARCHAR(255),position_url varchar(255)charset utf8  )')
        sql = ('insert into boss(company,position,address,position_url)VALUES (%s,%s,%s,%s)')
        data = (self.item['company'],self.item['position'],self.item['address'],self.item['position_url'])
        cursor.execute(sql,data)
        print('写入{}公司的{}岗位信息成功'.format(self.item['company'],self.item['position']))
        db.commit()


db = pymysql.connect(#连接数据库的参数
    host='localhost',
    user='root',
    password='123',
    charset='utf8',
    db='Boss',
)

cursor = db.cursor()#获取游标
boss = Boss()
boss.get_city()
cursor.close()#关闭游标
db.close()#关闭连接