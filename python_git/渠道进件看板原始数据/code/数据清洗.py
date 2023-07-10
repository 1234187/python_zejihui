import pandas as pd
import numpy as np
import datetime
import time
import warnings
from sqlalchemy import create_engine
from Sql.qfsql import qfsql
qf=qfsql()
warnings.filterwarnings('ignore')

# 获取当前时间的前一天和月份
def get_time():
    time_now=datetime.datetime.now()
    week_day=datetime.datetime.now().weekday()
    time_yesterday=time_now-datetime.timedelta(1)
    month = time_yesterday.month
    if month < 10:
        month='0'+str(month)
    else:
        month=month
    time_yesterday=time_yesterday.strftime('%Y-%m-%d')
    return time_yesterday,month



# 数据加载方法
def load_data(start_time,end_time):
    sql=f'''
    select
    or1.order_number,
    case when or1.state regexp 'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|buyout_finished'
             then 1 else 0 end as '是否成交',
    round(or1.cost/100+or1.accident_insurance/100,2) as '订单金额',
    case when or1.channel='' then '支付宝小程序'
        when or1.channel is null then '支付宝小程序'
        when ch.name is null then '支付宝小程序'
        when ch.name='连邦' then '连邦租机'
        when ch.name='魔盒' then '魔盒租机'
        when ch.name='元气渠道' then '元气租机'
        when ch.name='宜兔' then '宜兔租'
        else ch.name end as '渠道',
    case when or1.app_id='2021002189690874' then '择机汇'
         when or1.app_id='2021003101668145' then '趣租机'
         when or1.app_id='2021003125658104' then '漫妙龄'
         when or1.app_id='2021003133601148' then '趣机免押租赁'
         when or1.app_id='2021003160602218' then '家家租机'
         when or1.app_id='2021003176693040' then '宜惠租机'
         when or1.app_id='2021003176689056' then '喜来租机'
         when or1.app_id='2021003129691572' then '理想租机'
        end as '小程序',
    case when or1.merchant_credit_suggest='SUCCESS' then '低风险'
         when or1.merchant_credit_suggest='VALIDATE' then '中低风险'
         when or1.merchant_credit_suggest='STRICT_VALIDATE' then '中高风险'
         when or1.merchant_credit_suggest='REJECT' then '高风险'
         when or1.merchant_credit_suggest is null then '空值'
        end as '风险等级'
from `order` or1 left join channel ch on or1.channel=ch.channel_num
where date(or1.create_time) between '{start_time}' and '{end_time}'
  and or1.order_number like 'DD%'
    '''
    data=qf.data_fetcher_ji(sql)
    return data

# 获取风险等级数据情况
def get_risk_data(data,risk_level):
    data_risk=data[data['风险等级']==risk_level]
    data_risk=data_risk.groupby(['渠道','小程序']).agg({'order_number':'count','是否成交':'sum'}).reset_index()
    # data_risk[f'成交率--{risk_level}']=data_risk['是否成交']/data_risk['order_number']
    # data_risk[f'成交订单金额--{risk_level}']=data_risk[data_risk['是否成交']==1].groupby(['渠道','小程序'])['订单金额'].sum().reset_index()['订单金额']
    data_risk.rename(columns={'order_number':f'订单量--{risk_level}','是否成交':f'成交量--{risk_level}'},inplace=True)
    return data_risk

# 数据清洗方法
def clean_data(data):
    # 合计数据情况
    data_zong=data.groupby(['渠道','小程序']).agg({'order_number':'count','是否成交':'sum'}).reset_index()
    data_zong['成交率_合计']=data_zong['是否成交']/data_zong['order_number']
    data_zong.rename(columns={'order_number':'订单量_合计','是否成交':'成交量_合计'},inplace=True)
    data_zong_amount=data[data['是否成交']==1].groupby(['渠道','小程序'])['订单金额'].sum().reset_index()
    data_zong['占比_合计']=data_zong['订单量_合计']/data_zong['订单量_合计'].sum()
    data_zong=pd.merge(data_zong,data_zong_amount,on=['渠道','小程序'],how='left')
    # # 风险等级数据情况
    # data_risk1=get_risk_data(data,'低风险')
    # data_risk2=get_risk_data(data,'中低风险')
    # data_risk3=get_risk_data(data,'中高风险')
    # data_risk4=get_risk_data(data,'高风险')
    # data_risk5=get_risk_data(data,'空值')
    # # 数据合并
    # data_zong=pd.merge(data_zong,data_risk1,on=['渠道','小程序'],how='left')
    # data_zong=pd.merge(data_zong,data_risk2,on=['渠道','小程序'],how='left')
    # data_zong=pd.merge(data_zong,data_risk3,on=['渠道','小程序'],how='left')
    # data_zong=pd.merge(data_zong,data_risk4,on=['渠道','小程序'],how='left')
    # data_zong=pd.merge(data_zong,data_risk5,on=['渠道','小程序'],how='left')
    print(data_zong.columns)
    data_zong.columns=['channel','app','order_num','order_tran','transaction_rate','proportion','order_amount']
    return data_zong

# 数据写入方法：
def write_data(data,num=1):
    conn=create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/zejihui_data_base?charset=utf8')
    if num==1:
        data.to_sql('channel_input_day', con=conn, if_exists='append', index=False)
    if num==2:
        data.to_sql('channel_input_week', con=conn, if_exists='append', index=False)
    if num==3:
        data.to_sql('channel_input_month', con=conn, if_exists='append', index=False)

def run_():
    time_now=datetime.datetime.now()
    time_yesterday=time_now-datetime.timedelta(days=1)
    time_yesterday=time_yesterday.strftime('%Y-%m-%d')
    week_day=time_now.weekday() # 获取今天是周几
    data_day=load_data(time_yesterday,time_yesterday)
    data_test_day=clean_data(data_day)
    data_test_day['create_time']=time_yesterday
    # write_data(data_test_day,num=1)
    # data_test_day.to_excel('../data/日数据.xlsx',index=False)
    # 判断今天是周几，如果是周一，则获取上周的数据，如果不是周一，则获取昨天的数据
    if week_day==0:
        time_monday=time_now-datetime.timedelta(days=7)
        time_monday=time_monday.strftime('%Y-%m-%d')
        time_sunday=time_now-datetime.timedelta(days=1)
        time_sunday=time_sunday.strftime('%Y-%m-%d')
        data_week=load_data(time_monday,time_sunday)
        data_test_week = clean_data(data_week)
        data_test_week['date_monday']=time_monday
        data_test_week['date_sunday']=time_sunday
        write_data(data_test_week,num=2)
    # 判断今天是几号，如果是1号，则获取上个月的数据，如果不是1号，则获取昨天的数据
    if time_now.day==1:
        # 获取上个月的第一天和最后一天
        time_last_month_first=time_now.replace(day=1, month=datetime.datetime.now().month - 1)
        time_last_month_first=time_last_month_first.strftime('%Y-%m-%d')
        time_last_month_last=time_now-datetime.timedelta(days=1)
        time_last_month_last=time_last_month_last.strftime('%Y-%m-%d')
        data_month=load_data(time_last_month_first,time_last_month_last)
        data_test_month = clean_data(data_month)
        data_test_month['date_month_first']=time_last_month_first
        data_test_month['date_month_last']=time_last_month_last
        write_data(data_test_month,num=3)

if __name__ == '__main__':
    run_()
    print('数据写入成功')
