import random
import time
import warnings
from urllib.parse import quote

import pandas as pd
import requests

warnings.filterwarnings('ignore')


def read_data():
    data=pd.read_csv('./人人租机商家数据.csv',encoding='utf-8-sig')
    df1=pd.DataFrame()
    for i in range(0,int(len(data['商家名称'].unique())/10)+1):
        for j in range(0,10):
            if i*10+j>=len(data['商家名称'].unique()):
                break
            company_name=data['商家名称'].unique()[i*10+j]
            quote_company_name=quote(company_name)
            df2=get_company_phone(quote_company_name)
            print(f'{company_name}--获取完成')
            df1=df1.append(df2,ignore_index=True)
            time.sleep(4)
        time.sleep(15)
    df1.to_csv('人人租机商家数据_电话号码_原始数据.csv',encoding='utf_8_sig',index=False)
    df3=pd.merge(data,df1,left_on='商家名称',right_on='公司名称',how='left')
    df3.to_csv('人人租机商家数据_电话号码.csv',encoding='utf_8_sig',index=False)


    if 0==1:
        for i in data['商家名称'].unique().tolist():
            print(i)
            quote_search_key=quote(i)
            df2=get_company_phone(quote_search_key)
            df1=df1.append(df2,ignore_index=True)
            time.sleep(0.5)
        df3=pd.merge(data,df1,left_on='商家名称',right_on='公司名称',how='left')
        df3.to_csv('人人租机商家数据_电话号码.csv',encoding='utf_8_sig',index=False)

def get_company_phone(quote_searchkey_):
    rand_number=random.randint(100,999)
    time_set=str(int(time.time()))
    t=time_set+str(int(rand_number))
    hearders = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF XWEB/6763',
    }
    token='3c477005aa8f56234a8fc9c5328c05f2'
    url = f'https://xcx.qcc.com/mp-weixin/forwardApp/v3/base/advancedSearch?token={token}&t={t}&pageIndex=1&needGroup=yes&insuredCntStart=&insuredCntEnd=&recCapitalBegin=&recCapitalEnd=&registCapiBegin=&registCapiEnd=&countyCode=&province=&sortField=&isSortAsc=&searchKey={quote_searchkey_}&searchIndex=default&industryV3='
    response=requests.get(url,headers=hearders)
    json_0=response.json()
    # print(json_0)
    result=json_0['result']['Result']
    dic1={}
    if result==[]:
        return None
    company_info=result[0]
    dic1['公司名称']=company_info['Name']
    dic1['公司法人']=company_info['OperName']
    dic1['公司状态']=company_info['Status']
    dic1['注册资本']=company_info['RegistCapi']
    dic1['电话号码']=company_info['ContactNumber']
    dic1['公司地址']=company_info['Address']
    dic1['公司邮箱']=company_info['Email']
    dic1['成立日期']=company_info['StartDate']
    df1=pd.DataFrame(dic1,index=[0])
    return df1



if __name__ == '__main__':
    read_data()