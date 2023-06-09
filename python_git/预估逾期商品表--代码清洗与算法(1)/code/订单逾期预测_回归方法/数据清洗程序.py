import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')



# 读取数据
def read_data(path):
    data = pd.read_excel(path)
    # print(data.columns)

    # 筛选字段
    old_data_subset=data[['订单号','下单用户','年龄','性别','下单店铺','下单时间','支付时间','收货地址省份','月租金额','是否免押','风险等级','是否逾期']]
    #删除空值
    old_data_subset=old_data_subset.dropna(subset=['年龄','性别','下单店铺','下单时间','支付时间','收货地址省份','月租金额','是否免押','风险等级','是否逾期'],how='any')
    #增加第几周字段
    old_data_subset['下单周']=old_data_subset['下单时间'].dt.week
    #增加下单天字段
    old_data_subset['下单天']=old_data_subset['下单时间'].dt.date
    return old_data_subset


def pd_merge(df1,df2, on=['下单店铺','收货地址省份'],how='left'):
    df=pd.merge(df1,df2,on=on,how=how)
    return df



# 风险等级成交订单占比--按周，训练集数据清洗
def clean_data_train(df1,week):
    df_zong=pd.DataFrame()
    #计算各风险等级成交订单占比
    df_order_count=df1[df1['下单周']==week].groupby(['下单店铺'])['订单号'].count().reset_index()
    df_order_count.rename(columns={'订单号':'成交订单数'},inplace=True)
    df_risk_level_order_count=df_order_count.copy()
    # df_risk_level_overdue_order_count = df_order_count.copy()

    for i in list(df1['风险等级'].unique()):
        #各风险等级订单数
        df_risk_level_order_count1=df1[(df1['风险等级']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_risk_level_order_count1.rename(columns={'订单号': f'{i}风险成交订单数'}, inplace=True)
        df_risk_level_order_count = pd.merge(df_risk_level_order_count, df_risk_level_order_count1, on='下单店铺',how='left')
        #各风险等级逾期订单数
        df_risk_level_overdue_order_count1 = df1[(df1['风险等级'] == f'{i}') & (df1['下单周'] == week) & (df1['是否逾期'] == '是')].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_risk_level_overdue_order_count1.rename(columns={'订单号': f'{i}风险逾期订单数'}, inplace=True)
        df_risk_level_order_count = pd.merge(df_risk_level_order_count, df_risk_level_overdue_order_count1, on='下单店铺',how='left')
    df_risk_level_order_count.fillna(0,inplace=True)
    # print(df_risk_level_order_count.columns)
    for i in list(df1['风险等级'].unique()):
        df_risk_level_order_count[f'{i}风险成交订单占比']=round(df_risk_level_order_count[f'{i}风险成交订单数']/df_risk_level_order_count['成交订单数'],3)
        df_risk_level_order_count[f'{i}风险逾期率']=round(df_risk_level_order_count[f'{i}风险逾期订单数']/df_risk_level_order_count[f'{i}风险成交订单数'],3)


    #计算各风险等级成交订单占比
    df_shop_order_user_count=df1[df1['下单周']==week].groupby(['下单店铺'])['下单用户'].nunique().reset_index()
    df_shop_order_user_count.rename(columns={'下单用户':'成交用户数'},inplace=True)
    #数据合并
    df_zong=pd_merge(df_risk_level_order_count,df_shop_order_user_count,on='下单店铺',how='left')

    #计算性别成交订单占比
    df_gender_order_count=df_order_count.copy()
    for i in list(df1['性别'].unique()):
        df_gender_order_proportion=df1[(df1['性别']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_gender_order_proportion.rename(columns={'订单号':f'{i}性成交订单数'},inplace=True)
        df_gender_order_count=pd.merge(df_gender_order_count,df_gender_order_proportion,on='下单店铺',how='left')
    df_gender_order_count.fillna(0,inplace=True)
    for i in list(df1['性别'].unique()):
        df_gender_order_count[f'{i}性成交订单占比'] = round(df_gender_order_count[f'{i}性成交订单数'] / df_gender_order_count['成交订单数'], 3)
    df_gender_order_count.drop('成交订单数',axis=1,inplace=True)
    df_zong=pd_merge(df_zong,df_gender_order_count,on='下单店铺',how='left')

    #计算成交订单的平均年龄
    df_average_age_transaction_orders=df1[df1['下单周']==week].groupby(['下单店铺'],as_index=False)['年龄'].mean()
    df_average_age_transaction_orders.rename(columns={'年龄':'成交订单平均年龄'},inplace=True)
    df_average_age_transaction_orders['成交订单平均年龄']=round(df_average_age_transaction_orders['成交订单平均年龄'],0)
    #数据合并
    df_zong=pd_merge(df_zong,df_average_age_transaction_orders,on='下单店铺',how='left')

    #计算成交订单的月租平均金额
    df_average_monthly_rental_amount=df1[df1['下单周']==week].groupby(['下单店铺'],as_index=False)['月租金额'].mean()
    df_average_monthly_rental_amount.rename(columns={'月租金额':'成交订单月租平均金额'},inplace=True)
    df_average_monthly_rental_amount['成交订单月租平均金额']=round(df_average_monthly_rental_amount['成交订单月租平均金额'],3)
    #数据合并
    df_zong=pd_merge(df_zong,df_average_monthly_rental_amount,on='下单店铺',how='left')

    #计算是否免押金成交订单占比
    df_deposit_order_count=df_order_count.copy()
    for i in list(df1['是否免押'].unique()):
        df_deposit_order_proportion=df1[(df1['是否免押']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_deposit_order_proportion.rename(columns={'订单号':f'{i}成交订单数'},inplace=True)
        df_deposit_order_count=pd.merge(df_deposit_order_count,df_deposit_order_proportion,on='下单店铺',how='left')
    df_deposit_order_count.fillna(0,inplace=True)
    for i in list(df1['是否免押'].unique()):
        df_deposit_order_count[f'{i}成交订单占比']=round(df_deposit_order_count[f'{i}成交订单数']/df_deposit_order_count['成交订单数'],3)
    df_deposit_order_count.drop('成交订单数',axis=1,inplace=True)
    df_zong=pd_merge(df_zong,df_deposit_order_count,on='下单店铺',how='left')

    #成交订单省份top1
    df_province_order_count=df1[df1['下单周']==week].groupby(['下单店铺','收货地址省份'],as_index=False)['订单号'].count()
    df_province_order_count=df_province_order_count.sort_values(by='订单号',ascending=False).groupby('下单店铺').first().reset_index()
    df_province_order_count=df_province_order_count[['下单店铺','收货地址省份']]
    df_province_order_count.rename(columns={'收货地址省份':'成交订单省份top1'},inplace=True)
    #数据合并
    df_zong=pd_merge(df_zong,df_province_order_count,on='下单店铺',how='left')
    df_zong['下单周']=week
    return df_zong


# 风险等级成交订单占比--按周，测试集数据清洗
def clean_data_test(df1,week):
    df_zong=pd.DataFrame()
    #计算各风险等级成交订单占比
    df_order_count=df1[df1['下单周']==week].groupby(['下单店铺'])['订单号'].count().reset_index()
    df_order_count.rename(columns={'订单号':'成交订单数'},inplace=True)
    df_risk_level_order_count=df_order_count.copy()
    # df_risk_level_overdue_order_count = df_order_count.copy()

    for i in list(df1['风险等级'].unique()):
        #各风险等级订单数
        df_risk_level_order_count1=df1[(df1['风险等级']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_risk_level_order_count1.rename(columns={'订单号': f'{i}风险成交订单数'}, inplace=True)
        df_risk_level_order_count = pd.merge(df_risk_level_order_count, df_risk_level_order_count1, on='下单店铺',how='left')
    df_risk_level_order_count.fillna(0,inplace=True)
    for i in list(df1['风险等级'].unique()):
        df_risk_level_order_count[f'{i}风险成交订单占比'] = round(
            df_risk_level_order_count[f'{i}风险成交订单数'] / df_risk_level_order_count['成交订单数'], 3)

    #计算各风险等级成交订单占比
    df_shop_order_user_count=df1[df1['下单周']==week].groupby(['下单店铺'])['下单用户'].nunique().reset_index()
    df_shop_order_user_count.rename(columns={'下单用户':'成交用户数'},inplace=True)
    #数据合并
    df_zong=pd_merge(df_risk_level_order_count,df_shop_order_user_count,on='下单店铺',how='left')

    #计算性别成交订单占比
    df_gender_order_count=df_order_count.copy()
    for i in list(df1['性别'].unique()):
        df_gender_order_proportion=df1[(df1['性别']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_gender_order_proportion.rename(columns={'订单号':f'{i}性成交订单数'},inplace=True)
        df_gender_order_count=pd.merge(df_gender_order_count,df_gender_order_proportion,on='下单店铺',how='left')
    df_gender_order_count.fillna(0,inplace=True)
    for i in list(df1['性别'].unique()):
        df_gender_order_count[f'{i}性成交订单占比'] = round(df_gender_order_count[f'{i}性成交订单数'] / df_gender_order_count['成交订单数'], 3)
    df_gender_order_count.drop('成交订单数',axis=1,inplace=True)
    df_zong=pd_merge(df_zong,df_gender_order_count,on='下单店铺',how='left')

    #计算成交订单的平均年龄
    df_average_age_transaction_orders=df1[df1['下单周']==week].groupby(['下单店铺'],as_index=False)['年龄'].mean()
    df_average_age_transaction_orders.rename(columns={'年龄':'成交订单平均年龄'},inplace=True)
    df_average_age_transaction_orders['成交订单平均年龄']=round(df_average_age_transaction_orders['成交订单平均年龄'],0)
    #数据合并
    df_zong=pd_merge(df_zong,df_average_age_transaction_orders,on='下单店铺',how='left')

    #计算成交订单的月租平均金额
    df_average_monthly_rental_amount=df1[df1['下单周']==week].groupby(['下单店铺'],as_index=False)['月租金额'].mean()
    df_average_monthly_rental_amount.rename(columns={'月租金额':'成交订单月租平均金额'},inplace=True)
    df_average_monthly_rental_amount['成交订单月租平均金额']=round(df_average_monthly_rental_amount['成交订单月租平均金额'],3)
    #数据合并
    df_zong=pd_merge(df_zong,df_average_monthly_rental_amount,on='下单店铺',how='left')

    #计算是否免押金成交订单占比
    df_deposit_order_count=df_order_count.copy()
    for i in list(df1['是否免押'].unique()):
        df_deposit_order_proportion=df1[(df1['是否免押']==f'{i}')&(df1['下单周']==week)].groupby(['下单店铺'])['订单号'].count().reset_index()
        df_deposit_order_proportion.rename(columns={'订单号':f'{i}成交订单数'},inplace=True)
        df_deposit_order_count=pd.merge(df_deposit_order_count,df_deposit_order_proportion,on='下单店铺',how='left')
    df_deposit_order_count.fillna(0,inplace=True)
    for i in list(df1['是否免押'].unique()):
        df_deposit_order_count[f'{i}成交订单占比']=round(df_deposit_order_count[f'{i}成交订单数']/df_deposit_order_count['成交订单数'],3)
    df_deposit_order_count.drop('成交订单数',axis=1,inplace=True)
    df_zong=pd_merge(df_zong,df_deposit_order_count,on='下单店铺',how='left')

    #成交订单省份top1
    df_province_order_count=df1[df1['下单周']==week].groupby(['下单店铺','收货地址省份'],as_index=False)['订单号'].count()
    df_province_order_count=df_province_order_count.sort_values(by='订单号',ascending=False).groupby('下单店铺').first().reset_index()
    df_province_order_count=df_province_order_count[['下单店铺','收货地址省份']]
    df_province_order_count.rename(columns={'收货地址省份':'成交订单省份top1'},inplace=True)
    #数据合并
    df_zong=pd_merge(df_zong,df_province_order_count,on='下单店铺',how='left')
    df_zong['下单周']=week
    return df_zong


#数据汇总--测试集
def clean_data_train_zong(old_data_subset):
    data_clean = pd.DataFrame()
    for i in list(old_data_subset['下单周'].unique()):
        df_clean_data = clean_data_train(old_data_subset, i)
        data_clean = pd.concat([data_clean, df_clean_data], axis=0)
        # data_clearn['下单周']=i
        print('第{}周数据清洗完成'.format(i))
    data_clean = data_clean[['下单店铺', '省份', '成交订单数', '低风险成交订单占比', '中低风险成交订单占比', '中高风险成交订单占比', '高风险成交订单占比', '成交用户数',
                               '男性成交订单占比', '女性成交订单占比', '成交订单平均年龄', '成交订单月租平均金额', '免押成交订单占比', '部分押成交订单占比',
                               '低风险逾期率', '中低风险逾期率', '中高风险逾期率', '高风险逾期率', '下单周']]
    data_clean.fillna(0, inplace=True)
    return data_clean


#数据汇总--测试集
def clean_data_test_zong(old_data_subset):
    data_clean = pd.DataFrame()
    for i in list(old_data_subset['下单周'].unique()):
        df_clean_data = clean_data_test(old_data_subset, i)
        data_clean = pd.concat([data_clean, df_clean_data], axis=0)
        # data_clearn['下单周']=i
        print('第{}周数据清洗完成'.format(i))
    data_clean = data_clean[['下单店铺', '省份', '成交订单数', '低风险成交订单占比', '中低风险成交订单占比', '中高风险成交订单占比', '高风险成交订单占比', '成交用户数',
                               '男性成交订单占比', '女性成交订单占比', '成交订单平均年龄', '成交订单月租平均金额', '免押成交订单占比', '部分押成交订单占比','下单周']]
    data_clean.fillna(0, inplace=True)
    return data_clean

if __name__ == '__main__':
    # 读取数据--训练数据读取
    old_data_subset_train=read_data(path='../data/训练原始数据.xlsx')
    data_clean=clean_data_train_zong(old_data_subset_train)
    data_clean.to_csv('../data/train/data_clean_train.csv',index=False,encoding='utf-8-sig')
    print('训练集数据清洗完成')
    # 读取数据--测试数据读取
    old_data_subset_train = read_data(path='../data/测试原始数据.xlsx')
    data_clean = clean_data_train_zong(old_data_subset_train)
    data_clean.to_csv('../data/test/data_clean_test.csv', index=False, encoding='utf-8-sig')
    print('测试集数据清洗完成')

