import pandas as pd
import warnings
import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns
from Sql.qfsql import qfsql

warnings.filterwarnings('ignore')
qf=qfsql()


def group_data(data,group_name):
    data1=data.groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    data1.columns=[group_name,'下单用户']
    data2=data[data['是否通过']==1].groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    data2.columns=[group_name,'通过用户']
    data3=data[data['是否支付']==1].groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    data3.columns=[group_name,'支付用户']
    data4=data[data['是否发货']==1].groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    data4.columns=[group_name,'发货用户']
    data5=data[data['是否最终成交']==1].groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    data5.columns=[group_name,'最终成交用户']
    # data6=data[data['是否最终成交']==0].groupby(group_name).agg({'user_name':'nunique'}).reset_index()
    # data6.columns=[group_name,'取消用户']
    data_zong=pd.merge(data1,data2,on=group_name,how='left')
    data_zong=pd.merge(data_zong,data3,on=group_name,how='left')
    data_zong=pd.merge(data_zong,data4,on=group_name,how='left')
    data_zong=pd.merge(data_zong,data5,on=group_name,how='left')
    # data_zong=pd.merge(data_zong,data6,on=group_name,how='left')
    data_zong.set_index(group_name,inplace=True)
    data_zong=data_zong.T
    # 计算转化率
    data_zong_1=pd.DataFrame()
    for i in data_zong.columns:
        data_zong_1[i]=data_zong[i]/data_zong[i][0]
    data_zong_1=data_zong_1.T
    for j in data_zong_1.columns:
        data_zong_1.rename(columns={j:j+'转化率'},inplace=True)
    return data_zong.T,data_zong_1




# 获取所在时间范围第几周
def get_week_number(start_time,date_string):
    date = datetime.datetime.strptime(date_string, "%Y/%m/%d")
    start_date = datetime.datetime.strptime(start_time, "%Y/%m/%d")
    week_number = (date - start_date).days // 7 + 1
    return week_number

# 数据加载方法
def load_data(start_time,end_time,type_num=1):
    sql_zong=f'''
    select
    order_number,
    user_name,
     case when merchant_credit_check_result='SUCCESS' then 1
        when merchant_credit_check_result is null and first_pay_time is not null then 1
        else 0 end as '是否通过',
    case when date(first_pay_time) is not null then 1 else 0 end as '是否支付',
    case when delivery_time is null and state='pending_send_goods' then 1
        when delivery_time is not null then 1
        else 0 end as '是否发货',
    case when state regexp  'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
                return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                buyout_finished' then 1 else 0 end as '是否最终成交',
    date_format(create_time,'%Y-%m') as '下单月份',
    date_format(create_time,'%Y/%m/%d') as '下单日期'
from `order`
where date(create_time) between '{start_time}' and '{end_time}'
    '''
    data=qf.data_fetcher_ji(sql_zong)
    # 合计
    dic1={}
    dic1['下单用户']=data['user_name'].nunique()
    dic1['通过用户']=data[data['是否通过']==1]['user_name'].nunique()
    dic1['支付用户']=data[data['是否支付']==1]['user_name'].nunique()
    dic1['发货用户']=data[data['是否发货']==1]['user_name'].nunique()
    dic1['最终成交用户']=data[data['是否最终成交']==1]['user_name'].nunique()
    # dic1['取消用户']=data[data['是否最终成交']==0]['user_name'].nunique()
    data1=pd.DataFrame(dic1,index=['用户数']).T
    data1['转化率']=data1['用户数']/data1.iloc[0,0]
    # 分月汇总
    if type_num==1:
        data2,data3=group_data(data,'下单月份')
        return data1,data2,data3
    # 分周汇总
    elif type_num==2:
        data['下单周数']=data['下单日期'].apply(lambda x:get_week_number(start_time,x))
        for i in data['下单周数'].unique():
            data.loc[data['下单周数']==i,'下单周数']=f'第{i}周'
        # print(data)
        data2,data3=group_data(data,'下单周数')
        return data1,data2,data3
    # 分日汇总
    elif type_num==3:
        data2,data3=group_data(data,'下单日期')
        return data1,data2,data3




# 数据保存方法
def save_data_1(dic1={},data_name='',yesterday=''):
    if not os.path.exists(f'../data/{yesterday}'):
        os.makedirs(f'../data/{yesterday}')
    with pd.ExcelWriter(f'../data/{yesterday}/{data_name}.xlsx') as witer:
        for k, v in dic1.items():
            v.to_excel(witer, sheet_name=k)
            print(f'{k}保存完成！！！')

# 画图方法
def draw_pic(data1=pd.DataFrame,data2=pd.DataFrame(),num=1,yesterday='',title=''):
    if not os.path.exists(f'../data/{yesterday}'):
        os.makedirs(f'../data/{yesterday}')
    # 设置显示中文
    rc = {'font.sans-serif': 'SimHei',
          'axes.unicode_minus': False}
    sns.set(style="white", font='SimHei', rc=rc,context='notebook')
    # 分月汇总
    if 0==1:
        if num==1:
            plt.figure(figsize=(20, 10))
            sns_line=sns.lineplot(data=data)
            sns_line.set_title(f'用户数据转化数据-{title}',fontsize=15)
            #保存图片
            sns_line.figure.savefig(f'../data/{yesterday}/用户数据转化数据-{title}.png')
    if num==1:
        fig,axis=plt.subplots(3,2,figsize=(20,20))
        fig.suptitle(f'用户数据转化数据-{title}',fontsize=20)
        for i in range(data1.shape[1]):
            row=i//2
            col=i%2
            ax=axis[row,col]
            ax.set_title(data1.columns[i],fontsize=15,color='blue')
            ax.set_xlabel('月份',fontsize=15)
            ax.set_ylabel('用户数',fontsize=15)
            sns_bar=sns.barplot(x=data1.index,y=data1.iloc[:,i],ax=ax,palette='summer')
            sns_bar.tick_params(axis='y',labelsize=15)
            ax2=sns_bar.twinx()
            ax2.set_ylabel('转化率',fontsize=15)
            sns_line=sns.lineplot(x=data2.index,y=data2.iloc[:,i],marker='o',color='red',linewidth=2.5)
            sns_line.tick_params(axis='y',labelsize=15)
        plt.tight_layout()
        # plt.title(f'用户数据转化数据-{title}',fontsize=15)
        plt.savefig(f'../data/{yesterday}/用户数据转化数据-{title}.png')
    # 汇总画图
    elif num==2:
        data_1=data1.reset_index()
        data_1.columns=['链路','用户数','转化率']
        fig,axis=plt.subplots(figsize=(10,10))
        axis.set_title(f'用户数据转化数据-{title}',fontsize=15)
        axis.set_xlabel('链路',fontsize=15)
        axis.set_ylabel('用户数',fontsize=15)
        sns_bar=sns.barplot(x='链路',y='用户数',data=data_1,palette='summer')
        sns_bar.tick_params(axis='y',labelsize=15)
        ax2=sns_bar.twinx()
        ax2.set_ylabel('转化率',fontsize=15)
        sns_line=sns.lineplot(x='链路',y='转化率',data=data_1,marker='o',color='red',linewidth=2.5)
        sns_line.tick_params(axis='y',labelsize=15)
    plt.tight_layout()
    plt.savefig(f'../data/{yesterday}/用户数据转化数据-{title}.png')


# 合并两个df中间填空格
def df_merge(df1,df2=pd.DataFrame(),index_key=''):
    # df1=df1.reset_index()
    # df2=df2.reset_index()
    # df1=df1.rename(columns={'index':index_key})
    # df2=df2.rename(columns={'index':index_key})
    df3=pd.DataFrame([[' ' for i in range(df1.shape[1])]],columns=df1.columns)
    df4=pd.DataFrame([df1.columns],columns=df1.columns)
    print(df2)
    df5=df1.append([df3,df4,df2])
    # df5.set_index(keys=index_key,inplace=True)
    return df5

# 获取相对应时间段的数据
def get_time_run():
    dic1={}
    today=datetime.date.today()
    # 获取一年前当前月份第一天
    latst_year_start_day=datetime.datetime.today().replace(day=1, month=datetime.datetime.now().month,
                                                            year=datetime.datetime.now().year - 1).strftime('%Y/%m/%d')
    print('latst_year_start_day',latst_year_start_day)
    # 获取上个月份的最后一天
    last_month_end_day=(datetime.datetime.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y/%m/%d')
    print('last_month_end_day',last_month_end_day)


    # 8周前的日期
    eight_weeks_ago=(today-datetime.timedelta(days=56)).strftime('%Y/%m/%d')
    print('eight_weeks_ago',eight_weeks_ago)
    # 7天前的日期
    seven_days_ago=(today-datetime.timedelta(days=7)).strftime('%Y/%m/%d')
    print('seven_days_ago',seven_days_ago)
    yesterday=today-datetime.timedelta(days=1)
    print('最近一年时间段:' + latst_year_start_day + '-' + last_month_end_day)
    print('最近8周时间段:' + eight_weeks_ago + '-' + yesterday.strftime('%Y/%m/%d'))
    print('最近7天时间段:' + seven_days_ago + '-' + yesterday.strftime('%Y/%m/%d'))
    # 获取今一年的数据
    data_year,data_year_by_month,data_year_by_month_radio=load_data(latst_year_start_day,last_month_end_day,type_num=1)
    data_eight_weeks,data_eight_weeks_by_week,data_eight_weeks_by_week_radio=load_data(eight_weeks_ago,yesterday,type_num=2)
    data_seven_days,data_seven_days_by_day,data_seven_days_by_day_radio=load_data(seven_days_ago,yesterday,type_num=3)
    print(data_eight_weeks_by_week_radio)
    # 画图
    draw_pic(data1=data_year,num=2,yesterday=yesterday,title='近一年_汇总')
    draw_pic(data1=data_eight_weeks,num=2,yesterday=yesterday,title='近8周_汇总')
    draw_pic(data1=data_seven_days,num=2,yesterday=yesterday,title='近7天_汇总')
    draw_pic(data1=data_year_by_month,data2=data_year_by_month_radio, num=1, yesterday=yesterday, title='近一年_分月')
    draw_pic(data1=data_eight_weeks_by_week,data2=data_eight_weeks_by_week_radio, num=1, yesterday=yesterday, title='近8周_分周')
    draw_pic(data1=data_seven_days_by_day,data2=data_seven_days_by_day_radio, num=1, yesterday=yesterday, title='近7天_分日')
    # 保存数据
    dic1['近一年_汇总'] = data_year
    dic1['近一年_分月'] = pd.concat([data_year_by_month, data_year_by_month_radio], axis=1)
    dic1['近8周'] = data_eight_weeks
    dic1['近8周_分周'] = pd.concat([data_eight_weeks_by_week, data_eight_weeks_by_week_radio], axis=1)
    dic1['近7天'] = data_seven_days
    dic1['近7天_分日'] = pd.concat([data_seven_days_by_day, data_seven_days_by_day_radio], axis=1)
    save_data_1(dic1=dic1, data_name=f'用户数据转化数据-{yesterday}', yesterday=yesterday)




if __name__ == '__main__':
    get_time_run()