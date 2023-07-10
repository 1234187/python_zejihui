import pandas as pd
import warnings
import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns
from Sql.qfsql import qfsql

warnings.filterwarnings('ignore')
qf=qfsql()

# 数据加载方法
def load_data(start_time,end_time,num=1):
    # 合计
    if num==1:
        #1,下单用户
        order_user_sql=f'''
        select
    count(distinct user_name) as '下单用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
        '''
        # 2，通过用户
        order_users_deal_sql=f'''
        select
    count(distinct user_name) as '通过用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and merchant_credit_check_result='SUCCESS'
        '''
        # 3，支付用户
        order_users_pay_sql=f'''
        select
    count(distinct user_name) as '支付用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and date(first_pay_time) between '{start_time}' and '{end_time}'
        '''
        # 4,发货用户
        send_users_sql=f'''
        select
    count(distinct user_name) as '发货用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and date(first_pay_time) between '{start_time}' and '{end_time}'
-- and date(delivery_time) between '{start_time}' and '{end_time}'
and delivery_time is not null
        '''
        # 5,最终成交用户
        final_deal_users_sql=f'''
        select
    count(distinct user_name) as '最终成交用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and state regexp  'running|running_overdue|returning|
                return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                buyout_finished'
        '''
        # 6,取消用户
        cancel_users_sql=f'''
        select
    count(distinct user_name) as '取消用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and order_number not in (
    select distinct order_number from `order`
    where date(create_time) between '{start_time}' and '{end_time}'
    and state regexp  'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
                return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                buyout_finished'
    )
        '''


        data_order_user=qf.data_fetcher_ji(order_user_sql).T
        data_order_deal_user=qf.data_fetcher_ji(order_users_deal_sql).T
        data_order_pay_user=qf.data_fetcher_ji(order_users_pay_sql).T
        data_order_send_user=qf.data_fetcher_ji(send_users_sql).T
        data_order_final_deal_user=qf.data_fetcher_ji(final_deal_users_sql).T
        # data_order_cancel_user=qf.data_fetcher_ji(cancel_users_sql).T
        data=pd.concat([data_order_user,data_order_deal_user,data_order_pay_user,data_order_send_user,
                        data_order_final_deal_user],axis=0)
        data.columns = ['用户数']
        data['转化率'] = data['用户数'] / data.iloc[0, 0]
        # for i in range(1,data.shape[0]):
        #     data.iloc[i,1]=data.iloc[i,0]/data.iloc[0,0]
        return data
    if num==2:
        # 1,下单用户_分月
        order_users_by_month_sql=f'''
       select
    date_format(create_time,'%Y-%m') as '月份',
    count(distinct user_name) as '下单用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
group by date_format(create_time,'%Y-%m')
        '''
        # 2,通过用户_分月
        deal_users_by_month_sql=f'''
        select
    date_format(create_time,'%Y-%m') as '月份',
    count(distinct user_name) as '通过用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and merchant_credit_check_result='SUCCESS'
group by date_format(create_time,'%Y-%m')
        '''
        # 3,支付用户_分月
        pay_users_by_month_sql=f'''
        select
    date_format(create_time,'%Y-%m') as '月份',
    count(distinct user_name) as '支付用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and date(first_pay_time) between '{start_time}' and '{end_time}'
group by date_format(create_time,'%Y-%m')
'''
        # 4,发货用户_分月
        send_users_by_month_sql = f'''
                select
            date_format(create_time,'%Y-%m') as '月份',
            count(distinct user_name) as '发货用户'
        from `order`
        where date(create_time) between '{start_time}' and '{end_time}'
        and date(first_pay_time) between '{start_time}' and '{end_time}'
        and delivery_time is not null
        group by date_format(create_time,'%Y-%m')
                '''
        # 5,最终成交用户_分月
        final_deal_users_sql = f'''
                select
            date_format(create_time,'%Y-%m') as '月份',
            count(distinct user_name) as '最终成交用户'
        from `order`
        where date(create_time) between '{start_time}' and '{end_time}'
        and state regexp  'running|running_overdue|returning|
                        return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                        buyout_finished'
        group by date_format(create_time,'%Y-%m')
                '''
        # 6,取消用户_分月
        cancel_users_sql = f'''
        select
    date_format(create_time,'%Y-%m') as '月份',
    count(distinct user_name) as '取消用户'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
and order_number not in (
    select distinct order_number from `order`
    where date(create_time) between '{start_time}' and '{end_time}'
    and state regexp  'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
                return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                buyout_finished'
    )
group by date_format(create_time,'%Y-%m')
        '''
        data_order_user_by_month=qf.data_fetcher_ji(order_users_by_month_sql)
        data_order_deal_user_by_month=qf.data_fetcher_ji(deal_users_by_month_sql)
        data_order_pay_user_by_month=qf.data_fetcher_ji(pay_users_by_month_sql)
        data_order_send_user_by_month=qf.data_fetcher_ji(send_users_by_month_sql)
        data_order_final_deal_user_by_month=qf.data_fetcher_ji(final_deal_users_sql)
        data_order_cancel_user_by_month=qf.data_fetcher_ji(cancel_users_sql)
        data=pd.merge(data_order_user_by_month,data_order_deal_user_by_month,on='月份',how='left')
        data=pd.merge(data,data_order_pay_user_by_month,on='月份',how='left')
        data=pd.merge(data,data_order_send_user_by_month,on='月份',how='left')
        data=pd.merge(data,data_order_final_deal_user_by_month,on='月份',how='left')
        # data=pd.merge(data,data_order_cancel_user_by_month,on='月份',how='left')
        return data.set_index('月份')

# 数据保存方法
def save_data_1(dic1={},data_name='',yesterday=''):
    if not os.path.exists(f'../data/{yesterday}'):
        os.makedirs(f'../data/{yesterday}')
    with pd.ExcelWriter(f'../data/{yesterday}/{data_name}.xlsx') as witer:
        for k, v in dic1.items():
            v.to_excel(witer, sheet_name=k)
            print(f'{k}保存完成！！！')


# 获取相对应时间段的数据
def clean_data():
    dic1={}
    today=datetime.date.today()
    # 获取一年前当前月份第一天
    latst_year_start_day=datetime.datetime.today().replace(day=1, month=datetime.datetime.now().month,
                                                            year=datetime.datetime.now().year - 1).strftime('%Y-%m-%d')
    print('latst_year_start_day',latst_year_start_day)
    # 获取上个月份的最后一天
    last_month_end_day=(datetime.datetime.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    print('last_month_end_day',last_month_end_day)
    # 8周前的日期
    eight_weeks_ago=(today-datetime.timedelta(days=56)).strftime('%Y-%m-%d')
    print('eight_weeks_ago',eight_weeks_ago)
    # 7天前的日期
    seven_days_ago=(today-datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    print('seven_days_ago',seven_days_ago)
    yesterday=today-datetime.timedelta(days=1)
    # 获取今一年的数据
    data_year=load_data(latst_year_start_day,last_month_end_day)
    data_year_by_month=load_data(latst_year_start_day,last_month_end_day,2)
    data_eight_weeks=load_data(eight_weeks_ago,yesterday)
    data_seven_days=load_data(seven_days_ago,yesterday)
    dic1['近一年_汇总']=data_year
    dic1['近一年_分月']=data_year_by_month
    dic1['近8周']=data_eight_weeks
    dic1['近7天']=data_seven_days
    save_data_1(dic1=dic1,data_name=f'用户数据转化数据-{yesterday}',yesterday=yesterday)
    # 画图
    draw_pic(data_year,num=2,yesterday=yesterday,title='用户数据转化数据-近一年_汇总')
    draw_pic(data_year_by_month,num=1,yesterday=yesterday,title='用户数据转化数据-近一年_分月')
    draw_pic(data_eight_weeks,num=2,yesterday=yesterday,title='用户数据转化数据-近8周')
    draw_pic(data_seven_days,num=2,yesterday=yesterday,title='用户数据转化数据-近7天')


# 画图方法
def draw_pic(data,num=1,yesterday='',title=''):
    if not os.path.exists(f'../data/{yesterday}'):
        os.makedirs(f'../data/{yesterday}')
    # 设置显示中文
    rc = {'font.sans-serif': 'SimHei',
          'axes.unicode_minus': False}
    sns.set(style="white", font='SimHei', rc=rc,context='notebook')
    # 分月汇总
    if num==1:
        plt.figure(figsize=(20, 10))
        sns_line=sns.lineplot(data=data)
        sns_line.set_title(f'用户数据转化数据-近一年_分月',fontsize=15)
        #保存图片
        sns_line.figure.savefig(f'../data/{yesterday}/用户数据转化数据-近一年_分月.png')
    # 汇总画图
    elif num==2:
        data_1=data.reset_index()
        data_1.columns=['链路','用户数','转化率']
        plt.figure(figsize=(15, 10))
        sns_bar=sns.barplot(x='链路',y='转化率',data=data_1)
        sns_bar.set_title(f'用户数据转化数据-{title}',fontsize=15)
        #保存图片
        sns_bar.figure.savefig(f'../data/{yesterday}/用户数据转化数据-{title}.png')

if __name__ == '__main__':
    clean_data()