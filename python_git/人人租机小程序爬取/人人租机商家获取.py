import hashlib
import json
import re
import time
import warnings
from urllib.parse import urlencode, quote

import pandas as pd
import requests

warnings.filterwarnings('ignore')

# 定义方法获取sign
class Get_sign():
    # 将字典转换为数组
    def queryToArray(self,query):
        result = []
        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                for v in value:
                    result.append({
                        'key': key,
                        'value': v
                    })
            else:
                result.append({
                    'key': key,
                    'value': value
                })
        return result
    # 将数组转换为字符串 用去前半部分的url拼接
    def queryToStr(self,query, method='GET'):
        if method == 'POST':
            return urlencode(query, '')
        else:
            return '&'.join(['{}={}'.format(k, v) for k, v in query.items()])

    # 将数组转换为字符串 用去后半部分的sign进行MD5加密
    def queryToStr_1(self,query, method='GET'):
        if method == 'POST':
            return urlencode(query, '')
        else:
            return ''.join(['{}{}'.format(k, v) for k, v in query.items()])

    # 获取sign 根据url、请求方式、公共参数、私有参数将公共参数机型参数进行排序，然后拼接成字符串，最后进行MD5加密
    def get_sign(self,url,method,public_params={},private_params={}):
        # 判断url是否为空
        if not url:
            return ''
        # 将公共参数和私有参数合并
        s={**public_params,**private_params}
        # 将合并后的参数转换为数组
        u=self.queryToArray(s) or []
        # 判断数组是否为空
        if len(u)==0:
            return url
        # 对数组进行排序
        u.sort(key=lambda x:x['key'])
        # 将数组转换为字典 用于拼接字符串
        u_dict={i['key']:i['value'] for i in u}
        sign_str=quote(urlencode(u_dict,'',''), safe='~()*!.=&\'')
        # 将数组转换为字符串 用去后半部分的sign进行MD5加密
        return '{}?{}&sign={}'.format(url, self.queryToStr(private_params if method == 'POST' else s, method), hashlib.md5('{}{}'.format(self.queryToStr_1(u_dict), 'L8aU4erNfY2xbIHy').encode()).hexdigest())





# 获取分类页数手机类型的数据
def get_phone_type(objece1,headers):
    url1='https://api.rrzuji.com/individual/cate/all-v4'
    method='GET'
    public_params={
        "mini_version": "1.13.33",
        "terminal": "wx.mini",
    }
    url=objece1.get_sign(url1,method,public_params)
    response=requests.get(url,headers=headers)
    json_0=response.json()
    # 获取分类页第一个类型下的数据
    type_0 = json_0['data'][0]
    df1=get_cate_type_0(headers,objece1,type_0)
    # 获取分类页第二个类型下的数据
    type_1=json_0['data'][1]
    df2=get_cate_type_1(headers,objece1,type_1)
    df1=df1.append(df2,ignore_index=True)
    df1.to_csv('人人租机商家数据.csv',encoding='utf_8_sig',index=False)
    print('数据保存完成')

# 获取分类页第一个分类的手机数据
def get_cate_type_0(headers,objece1,type_0):
    df1 = pd.DataFrame()
    # 获取分类类型名称
    menu_name = type_0['menuName']
    tab_data = type_0['tabData']
    for i in tab_data:
        # 类型下个活动版块的名称
        tab_class = i['tab_class']
        # 类型下个活动版块的的品牌名称
        tab_list = i['tab_list']
        # 遍历个品牌获取各品牌的详细数据
        for brand in tab_list:
            # 分类名称
            tab_name = brand['name']
            # 品牌名称
            brand_name = re.findall('.*?brand=(.*).*?',brand['api_url'])[0]
            # 品牌ID
            brand_id = re.findall('.*?brand_id=(.*).*?',brand['api_url'])
            if len(brand_id) == 0:
                brand_id = ''
            else:
                brand_id = brand_id[0]
            # 各品牌携带的参数
            brand_position = brand['position_sign']
            # 获取该品牌下面第一页的手机数据
            if 1 == 1:
                for item in brand['item_list']:
                    item_id = item['item_id']
                    df2 = get_company_name(headers, item_id, menu_name)
                    df1 = df1.append(df2, ignore_index=True)
                    print(f'{menu_name}-{tab_class}-{brand_name}--{item_id},获取完成')
                    time.sleep(1)
            # 调用获取搜索页各手机类型下手机的id
            df3 = get_search_phone_id(objece1, headers, brand_name, menu_name, brand_position, tab_class,tab_name, brand_id)
            df1 = df1.append(df3, ignore_index=True)
    return df1

# 获取二手手机下面的所有数据
def get_cate_type_1(headers,objece1,type_1):
    df1 = pd.DataFrame()
    # 获取分类类型名称
    menu_name = type_1['menuName']
    tab_data = type_1['tabData']
    tab_data_0=tab_data[0]
    tab_data_0_list=tab_data_0['tab_list']
    for i in tab_data_0_list:
        j=i['api_url']
        acctivity_id=re.findall('.*?id=(.*).*?',j)[0]
        df2=get_cate_type_1_activy(headers,objece1,acctivity_id,menu_name)
        df1=df1.append(df2,ignore_index=True)
    # 获取二手手机下面展示的机型数据
    for i in range(1,3):
        tab_list=tab_data[i]['tab_list']
        for i in tab_list:
            item_id=i['api_url']
            df3=get_company_name(headers,item_id,menu_name)
            df1=df1.append(df3,ignore_index=True)
    return df1


# 获取第二类分类页活动页数据
def get_cate_type_1_activy(headers,objece1,page_id,menu_name):
    df1=pd.DataFrame()
    url='https://api.rrzuji.com/individual/h5/application-page'
    method='GET'
    public_params={
        'id':page_id,
        'mini_version':'1.13.33',
        'terminal':'wx.mini'
    }
    url=objece1.get_sign(url,method,public_params)
    response=requests.get(url,headers=headers)
    json_0=response.json()
    json_0=json.dumps(json_0)
    item_id=re.findall('.*?"item_id": "(.*?)".*?',json_0,)
    for i in item_id:
        if i =='':
            continue
        df2=get_company_name(headers,i,menu_name)
        df1=df1.append(df2,ignore_index=True)
        time.sleep(0.5)
    return df1


# 获取搜索页各手机类型下手机的id
def get_search_phone_id(objece1,headers,brand_name,menu_name,page_position_sign,tab_class,tab_name,brand_id=''):
    url='https://api.rrzuji.com/individual/search/product'
    method='GET'
    if brand_id=='':
        public_params={
            'brand':brand_name,
            'cate':'phone',
            'menu_name':menu_name,
            'page_position_sign':page_position_sign,
            'tab_class':tab_class,
            'tab_name':tab_name,
            'page':1,
            'page_size':1000,
            'short':'all',
            'city':'全国',
            'mini_version':'1.13.33',
            'terminal':'wx.mini'
        }
    else:
        public_params={
            'brand':brand_name,
            'brand_id':brand_id,
            'cate':'phone',
            'menu_name':menu_name,
            'page_position_sign':page_position_sign,
            'tab_class':tab_class,
            'tab_name':tab_name,
            'page':1,
            'page_size':1000,
            'short':'all',
            'city':'全国',
            'mini_version':'1.13.33',
            'terminal':'wx.mini'
        }
    df1=pd.DataFrame()
    # 生成签名并且拼接成网站
    url=objece1.get_sign(url,method,public_params)
    response=requests.get(url,headers=headers)
    json_0=response.json()
    for i in json_0['data']['list']:
        item_id=i['id']
        df2=get_company_name(headers,item_id,menu_name)
        df1=df1.append(df2,ignore_index=True)
        print(f'{menu_name}-{tab_class}-{brand_name}--{item_id},获取完成')
        time.sleep(1)
    return df1


def get_company_name(headers,id,menu_name):
    url1='https://api.rrzuji.com/individual/item/view'
    method = "GET"
    public_params = {
        'id': id,
        'userId': 47669682,
        "mini_version": "1.13.33",
        "terminal": "wx.mini",
    }
    url= get_sign.get_sign(url1, method, public_params)
    response=requests.get(url,headers=headers)
    json_1=response.json()
    # print(json_1)
    df1=pd.DataFrame()
    dic1={}
    try:
        i=json_1['data']['model']
        dic1['商品分类']=menu_name
        dic1['商品id']=id
        dic1['商品名称']=i['scheme_title']
        dic1['品牌名称']=i['brand_name']
        dic1['机型'] = i['model']
        dic1['店铺名称']=i['server_name']
        dic1['商家名称']=i['company_name']
        dic1['商品押金']=i['deposit']
        dic1['租赁价格/天']=i['rental']
        dic1['买断价']=i['buyout']
        df2=pd.DataFrame(dic1,index=[0])
        df1=df1.append(df2,ignore_index=True)
        return df1
    except Exception as e:
        print(e)
        print('商品id为'+str(id)+'的商品数据下架了')





if __name__ == '__main__':
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF XWEB/6762',
        'token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2ODI3NzAxMDcsInN1YiI6bnVsbCwibmJmIjoxNjgyMTY1MzA3LCJhdWQiOjQ3NjY5NjgyLCJpYXQiOjE2ODIxNjUzMDcsImp0aSI6IjJWUlBlQ1dvMDEiLCJpc3MiOiJFYXN5U3dvb2xlIiwic3RhdHVzIjoxLCJkYXRhIjpudWxsfQ.mTqLGLW8D0kAb4tzggKAKg6gwJCXRtq7bO3LC7wR3bU'
    }
    get_sign=Get_sign()
    # get_product_id(get_sign,headers,)
    get_phone_type(get_sign,headers)
