import os
import warnings

import pandas as pd
from Sql.qfsql import qfsql

qf=qfsql()
warnings.filterwarnings('ignore')
# 数据加载--申请订单数据
def load_data(start_time,end_time):
    load_data_sql=f"""
        select
       or1.order_number as '订单号',
       or1.user_name as '下单用户',
       us1.sex as '性别',
       case when us1.birthday is null then null 
       else year(now())-year(us1.birthday) end as '年龄',
       case when year(now())-year(birthday) >0 and year(now())-year(birthday) <=18 then '0-18'
           when year(now())-year(birthday)>18 and year(now())-year(birthday)<=22 then '19-22'
           when year(now())-year(birthday)>22 and year(now())-year(birthday)<=35 then '23-35'
           when year(now())-year(birthday)>36 then '36-inf'
           end as '年龄区间',
       mer1.merchant_name as '下单店铺',
       or1.create_time as '下单时间',
       date (or1.create_time) as '下单日期',
       or1.first_pay_time as '支付时间',
       round(or1.lease_term/30,0) as '租赁期限',
       oe1.receive_address as '收货地址',
       oe1.provice as '收货地址--省份',
       oe1.city as '收货地址--城市',
       oe1.regoin as '收货地址--区县',
       round(or1.daily_rent/100*30,2) as '每月租金',
       case when round(daily_rent/100*30,1)>0 and round(daily_rent/100*30,1)<=650 then '0-650'
           when round(daily_rent/100*30,1)>650 and round(daily_rent/100*30,1)<=1100 then '650-1100'
#            when round(daily_rent/100*30,1)>1000 and round(daily_rent/100*30,1)<=1500 then '1000-1500'
           when round(daily_rent/100*30,1)>1100 then '1100-inf'
       end as '月租金额区间',
       round(og1.price/100,2) as '设备价格',
       round(or1.cost/100+or1.accident_insurance/100,2) as '订单总金额',
       case when or1.deposit_type='ALL_DEPOSIT' then '全押'
            when or1.deposit_type='PARTIAL_DEPOSIT' then '部分押'
            when or1.deposit_type='FREE_DEPOSIT' then '免押'
       end as '押金类型',
       or1.deposit/100 as '押金金额',
       case when or1.channel='' then '支付宝小程序'
            when or1.channel is null then '支付宝小程序'
            else ch1.name end as '渠道',
       og1.goods_name as '商品名称',
       og1.model as '机型',
       og1.category as '一级分类',
       og1.type as '二级分类',
       og1.brand as '品牌',
       og1.old_level as '新旧程度',
       og1.standard as '规格',
       round(or1.accident_insurance/100,1) as '碎屏保险',
       case when or1.merchant_credit_suggest='SUCCESS' then '低风险'
           when or1.merchant_credit_suggest='VALIDATE' then '中低风险'
           when or1.merchant_credit_suggest='STRICT_VALIDATE' then '中高风险'
           when or1.merchant_credit_suggest='REJECT' then '高风险'
           when or1.merchant_credit_suggest is null then null
           end as '风险等级',
       case
           when or1.state='pending_submit' then '申请中'
           when or1.state='pending_system_credit_check' then '待系统审核'
           when or1.state='pending_jimi_credit_check' then '待机蜜信用审核'
           when or1.state='pending_artificial_credit_check' then '待人工审核'
           when or1.state='pending_transfer' then '待转单'
           when or1.state='pending_order_receiving' then '待接单'
           when or1.state='pending_sign' then '待签署'
           when or1.state='pending_gongzheng_sign' then '协议公证待签署'
           when or1.state='pending_pay' then '待支付'
           when or1.state='pending_send_goods' then '待发货'
           when or1.state='pending_receive_goods' then '待收货'
           when or1.state='pending_running' then '待开始'
           when or1.state='express_rejection' then '快递拒签'
           when or1.state='running' then '租赁中'
           when or1.state='pending_relet_check' then '续租审核中'
           when or1.state='pending_relet_pay' then '续租待支付'
           when or1.state='pending_relet_start' then '续租待开始'
           when or1.state='running_overdue' then '租赁逾期'
           when or1.state='pending_return' then '到期待还'
           when or1.state='returning' then '退还中'
           when or1.state='return_overdue' then '退还逾期'
           when or1.state='returned_unreceived' then '退还中'
           when or1.state='returned_received' then '商家已收货'
           when or1.state='systems_disposal' then '平台处理中'
           when or1.state='pending_compensate_check' then '赔偿待审核'
           when or1.state='compensate_overdue' then '赔偿逾期'
           when or1.state='pending_user_compensate' then '等待用户赔偿'
           when or1.state='pending_refund_deposit' then '退还押金中'
           when or1.state='pending_refund_deposit_check' then '退还押金审核中'
           when or1.state='lease_finished' then '租赁完成'
           when or1.state='relet_finished' then '租赁完成'
           when or1.state='system_credit_check_unpass_canceled' then '机审取消'
           when or1.state='merchant_credit_check_unpass_canceled' then '机审取消'
           when or1.state='artificial_credit_check_unpass_canceled' then '人审取消'
           when or1.state='merchant_relet_check_unpass_canceled' then '商户审核不通过取消'
           when or1.state='user_canceled' then '用户主动取消'
           when or1.state='safeguard_rights_canceled' then '维权取消'
           when or1.state='order_payment_overtime_canceled' then '订单支付超时取消'
           when or1.state='merchant_not_yet_send_canceled' then '商户未发货订单取消'
           when or1.state='express_rejection_canceled' then '快递拒签取消'
           when or1.state='return' then '待退货'
           when or1.state='refund' then '退款中'
           when or1.state='pending_buyout_pay' then '待买断'
           when or1.state='after_sales_retruning' then '退货中'
           when or1.state='after_sales_retruned_unreceived' then '已退货（未收到货）'
           when or1.state='after_sales_system_disposal' then '平台处理中'
           when or1.state='after_sales_returned_received' then '已退货'
           when or1.state='repairing' then '维修中'
           when or1.state='return_goods' then '退货中'
           when or1.state='exchange_goods' then '换货中'
           when or1.state='bad_debt_running' then '坏账中'
           when or1.state='bad_debt_finished' then '坏账完结'
           when or1.state='buyout_finished' then '买断完成'
       end as '订单状态',
       case when or1.app_id='2021002189690874' then '择机汇'
            when or1.app_id='2021003101668145' then '趣租机'
            when or1.app_id='2021003125658104' then '漫妙龄'
            when or1.app_id='2021003133601148' then '趣机免押租赁'
            when or1.app_id='2021003160602218' then '家家租机'
            when or1.app_id='2021003176693040' then '宜惠租机'
            when or1.app_id='2021003176689056' then '喜来租机'
            when or1.app_id='2021003129691572' then '理想租机'
           end as '小程序',
       case when or1.state='running_overdue' then 1
           else 0 end as '是否逾期',
       case when or1.state regexp 'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|buyout_finished'
           then 1 else 0 end as '是否通过',
       oo1.delay_day as '逾期天数',
       oo1.delay_amount as '逾期金额'
# from `order` as or1 left join user as us1 on or1.user_id=us1.id #用户表
from `order` as or1 left join (select id,phone,sex,birthday from user where sex is not null group by phone) as us1 on or1.user_name=us1.phone #用户表
left join merchant as mer1 on or1.merchant_id=mer1.id #商户表
left join order_express as oe1 on or1.order_number=oe1.order_number #订单物流表
left join order_goods as og1 on or1.order_number=og1.order_number #订单商品表
left join channel as ch1 on or1.channel=ch1.channel_num #渠道表
left join order_overdue as oo1 on or1.order_number=oo1.order_number #订单逾期表
where date(or1.create_time) between '{start_time}' and '{end_time}'
and or1.order_number like 'DD%'
    """
    zong_data=qf.data_fetcher_ji(load_data_sql)
    zong_data[['性别','年龄','年龄区间','风险等级']]=zong_data[['性别','年龄','年龄区间','风险等级']].fillna('空值')
    return zong_data

# 数据加载--逾期订单数据
def load_data_overdue(start_time,end_time):
    load_data_overdue=f"""
   select
#        count(distinct py1.order_number) as '订单数',
       py1.order_number as '订单号',
       py1.bill_start_time as '账单开始时间',
       oo.create_time as '首次逾期时间',
       py1.delay_start_time as '逾期开始时间',
       py1.delay_end_time as '逾期结束时间',
       py1.user_name as '下单用户',
       or1.merchant_name as '商户',
       case when or1.state='running_overdue' then 1 else 0 end as '是否逾期',
       case when round(or1.daily_rent/100*30,1)>0 and round(or1.daily_rent/100*30,1)<=650 then '0-650'
           when round(or1.daily_rent/100*30,1)>650 and round(or1.daily_rent/100*30,1)<=1100 then '650-1100'
#            when round(daily_rent/100*30,1)>1000 and round(daily_rent/100*30,1)<=1500 then '1000-1500'
           when round(or1.daily_rent/100*30,1)>1100 then '1100-inf'
       end as '月租金额区间',
       case when year(now())-year(us1.birthday) >0 and year(now())-year(us1.birthday) <=18 then '0-18'
           when year(now())-year(us1.birthday)>18 and year(now())-year(us1.birthday)<=22 then '19-22'
           when year(now())-year(us1.birthday)>22 and year(now())-year(us1.birthday)<=35 then '23-35'
           when year(now())-year(us1.birthday)>36 then '36-inf'
           end as '年龄区间',
       us1.sex as '性别',
       oe.provice as '收货地址--省份',
       oe.city as '收货地址--城市',
       oe.regoin as '收货地址--区县',
       case when merchant_credit_suggest='SUCCESS' then '低风险'
           when merchant_credit_suggest='VALIDATE' then '中低风险'
           when merchant_credit_suggest='STRICT_VALIDATE' then '中高风险'
           when merchant_credit_suggest='REJECT' then '高风险'
           when merchant_credit_suggest is null then null
           end as '风险等级',
       og.goods_name as '商品名称',
       og.category as '一级分类',
       og.type as '二级分类',
       og.brand as '品牌',
       og.model as '型号',
       og.standard as '规格',
       og.old_level as '新旧程度'
from payment py1 left join `order` or1 on py1.order_number= or1.order_number
left join (select id,phone,sex,birthday from user where sex is not null group by phone) as us1 on py1.user_name=us1.phone
left join order_express oe on oe.order_number=py1.order_number
left join order_goods og on py1.order_id=og.order_id
left join order_overdue oo on or1.order_number = oo.order_number
where or1.state regexp 'pending_send_goods|pending_receive_goods|running|running_overdue'
and date(bill_start_time) between '{start_time}' and '{end_time}'
    """
    accounting_period_order_data=qf.data_fetcher_ji(load_data_overdue)
    # 缺失值填充
    accounting_period_order_data[['性别','年龄区间','风险等级']]=accounting_period_order_data[['性别','年龄区间','风险等级']].fillna('空值')
    return accounting_period_order_data

# 数据保存方法
def save_data_1(dic1={},month='',data_name=''):
    if not os.path.exists(f'../data/save_data/{month}'):
        os.makedirs(f'../data/save_data/{month}')
    with pd.ExcelWriter(f'../data/save_data/{month}/{data_name}.xlsx') as witer:
        for k, v in dic1.items():
            v.to_excel(witer, sheet_name=k)
            print(f'{k}保存完成！！！')

# 申请订单类
class ApplyForAnOrder():
    # 分组方法
    def data_group_by(self,data,group_fields,group_fields_1='',data2='',num=1):
        if num==1:
            if group_fields_1=='':
                data1 = data.groupby([group_fields]).agg({'订单号': 'count', '下单用户': 'nunique', '是否通过': 'sum'}).reset_index()
            else:
                data1=data.groupby([group_fields,group_fields_1]).agg({'订单号': 'count', '下单用户': 'nunique', '是否通过': 'sum'}).reset_index()
            data1['通过率'] = round((data1['是否通过'] / data1['订单号']) * 100, 2)
            data1 = data1.sort_values(by='订单号', ascending=False)
            return data1
        elif num==2:
            # 计算环比增长
            data_1=data.groupby([group_fields]).agg({'订单号': 'count', '下单用户': 'nunique', '是否通过': 'sum'}).reset_index()
            data_2=data2.groupby([group_fields]).agg({'订单号': 'count', '下单用户': 'nunique', '是否通过': 'sum'}).reset_index()
            data_1['通过率'] = round((data_1['是否通过'] / data_1['订单号']) * 100, 2)
            data_2['通过率'] = round((data_2['是否通过'] / data_2['订单号']) * 100, 2)
            data_1['占比']=round((data_1['订单号']/data_1['订单号'].sum()),2)
            data_zong=pd.merge(data_1,data_2,on=group_fields,suffixes=('_1','_2'),how='left')
            data_zong['订单环比增长']=round((data_zong['订单号_1']/data_zong['订单号_2']-1),2)
            # print(data_zong)
            data_zong=data_zong[[group_fields,'订单号_1','下单用户_1','是否通过_1','通过率_1','占比','订单环比增长','订单号_2','下单用户_2','是否通过_2','通过率_2']]
            data_zong.rename(columns={'订单号_1':'订单数','下单用户_1':'下单用户数','是否通过_1':'通过订单数','通过率_1':'通过率'},inplace=True)
            data_zong=data_zong.sort_values(by='订单数',ascending=False)
            return data_zong
    # 数据整体情况
    def tab_zong(self,data,data1):
        dic1={}
        # 申请订单数
        number_applied_orders_current_month=len(data['订单号'].unique())
        number_applied_orders_last_month=len(data1['订单号'].unique())
        # 下单用户数
        number_users_placing_orders_current_month=len(data['下单用户'].unique())
        number_users_placing_orders_last_month=len(data1['下单用户'].unique())
        # 通过订单数
        number_orders_pass_current_month=len(data[data['是否通过']==1]['订单号'].unique())
        number_orders_pass_last_month=len(data1[data1['是否通过']==1]['订单号'].unique())
        dic1['申请订单数_当前月份']=number_applied_orders_current_month
        dic1['下单用户数_当前月份']=number_users_placing_orders_current_month
        dic1['通过订单数_当前月份']=number_orders_pass_current_month
        dic1['通过率_当前月份']=round((number_orders_pass_current_month/number_applied_orders_current_month),2)
        dic1['申请订单数_上月份']=number_applied_orders_last_month
        dic1['下单用户数_上月份']=number_users_placing_orders_last_month
        dic1['通过订单数_上月份']=number_orders_pass_last_month
        dic1['环比增长率_订单数']=round((number_applied_orders_current_month/number_applied_orders_last_month-1),2)
        data1=pd.DataFrame(dic1,index=[0])
        return data1

    # 每天申请订单情况
    def tab_everyday(self,data):
        data1=data.groupby(['下单日期']).agg({'订单号':'count','下单用户':'nunique','是否通过':'sum'}).reset_index()
        data1['通过率']=round((data1['是否通过']/data1['订单号']),2)
        return data1

    # 各风险等级分单情况
    def tab_risk_level(self,data):
        data1=data.groupby(['风险等级']).agg({'订单号':'count','下单用户':'nunique','是否通过':'sum'}).reset_index()
        data1['通过率']=round((data1['是否通过']/data1['订单号']),2)
        data1['订单占比']=round((data1['订单号']/data1['订单号'].sum()),2)
        return data1

    # 订单商品数据分布情况
    def tab_goods(self,data):
        # data['内存']=data['规格'].map(lambda x:re.findall('.*?"name":"(.*?)","scname":"内存".*?',x))
        # data['内存']=data['内存'].map(lambda x:x[0] if len(x)>0 else '')
        data1=data.groupby(['品牌','机型']).agg({'订单号':'count','下单用户':'nunique','是否通过':'sum'}).reset_index()
        data1['通过率']=round((data1['是否通过']/data1['订单号']),2)
        data1=data1.sort_values(by=['品牌','订单号'],ascending=[False,False])
        data1=data1.groupby(['品牌']).head(6)
        return data1

    # 申请订单省份--城市分布情况
    def tab_province_city(self,data):
        data1=data.groupby(['收货地址--省份','收货地址--城市']).agg({'订单号':'count','下单用户':'nunique','是否通过':'sum'}).reset_index()
        data1['通过率']=round((data1['是否通过']/data1['订单号']),2)
        data1=data1.sort_values(by=['收货地址--省份','订单号'],ascending=[False,False])
        # data1=data1.groupby(['省份']).head(6)
        return data1

    # 获取新注册的用户和用户转化数据
    def user_conversion(self,start_time,end_time):
        dic1={}
        # 新用户注册数
        sql1=f"""
        select count(distinct phone) from user where date(create_time) between '{start_time}' and '{end_time}'
        """
        # 申请订单数
        sql2=f"""
        select count(distinct phone) from user where date(create_time) between '{start_time}' and '{end_time}'
        and phone in (select distinct user_name from `order` where date(create_time) between '{start_time}' and '{end_time}')
        """
        # 通过订单数
        sql3=f"""
        select count(distinct phone) from user where date (create_time) between '{start_time}' and '{end_time}'
        and phone in(select distinct user_name from `order` where date(create_time) between '{start_time}' and '{end_time}'
        and state regexp 'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
        return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|buyout_finished')
        """
        dic1['新用户注册数']=qf.data_fetcher_ji(sql1).values[0]
        dic1['申请用户数']=qf.data_fetcher_ji(sql2).values[0]
        dic1['通过的用户数']=qf.data_fetcher_ji(sql3).values[0]
        data1=pd.DataFrame(dic1,index=['人数']).T
        return data1

# 账期订单类
class AccountingPeriodOrder():
    # 账期订单整体分布情况
    def tab_account(self,data):
        dic1={}
        dic1['账期订单数']=len(data['订单号'].unique())
        dic1['账期下单用户数']=len(data['下单用户'].unique())
        dic1['逾期订单数']=len(data[data['是否逾期']==1]['订单号'].unique())
        dic1['逾期率']=round((dic1['逾期订单数']/dic1['账期订单数']),2)
        data1=pd.DataFrame(dic1,index=[0])
        return data1

    # 各风险等级订单逾期情况
    def tab_risk_level_account(self,data):
        data1=data.groupby(['风险等级']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1['订单占比']=round((data1['订单号']/data1['订单号'].sum()),2)
        return data1

    # 账期订单省份分布情况
    def tab_province_account(self,data):
        data1=data.groupby(['收货地址--省份']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1['订单占比']=round((data1['订单号']/data1['订单号'].sum()),2)
        data1=data1.sort_values(by='订单号',ascending=False)
        # data1=data1.head(6)
        return data1

    # 账期订单用户属性分布情况
    def tab_user_account(self,data):
        data1=data.groupby(['性别']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1['订单占比']=round((data1['订单号']/data1['订单号'].sum()),2)
        data2=data.groupby(['年龄区间']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data2['逾期率']=round((data2['是否逾期']/data2['订单号']),2)
        data2['订单占比']=round((data2['订单号']/data2['订单号'].sum()),2)
        return data1,data2

    # 账期订单月租金额区间分布
    def tab_rent_account(self,data):
        data1=data.groupby(['月租金额区间']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1['订单占比']=round((data1['订单号']/data1['订单号'].sum()),2)
        return data1

    # 账期订单商品属性分布情况
    def tab_goods_account(self,data):
        data1=data.groupby(['品牌','型号']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1=data1.sort_values(by=['品牌','订单号'],ascending=[False,False])
        data1=data1.groupby(['品牌']).head(6)
        return data1

    # 账期订单省份-城市分布情况
    def tab_province_city_account(self,data):
        data1=data.groupby(['收货地址--省份','收货地址--城市']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1=data1.sort_values(by=['收货地址--省份','逾期率'],ascending=[False,False])
        # data1=data1.groupby(['收货地址--省份']).head(6)
        return data1

    # 账期订单商户分布情况
    def tab_merchant_account(self,data):
        data1=data.groupby(['商户']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1=data1.sort_values(by=['订单号'],ascending=False)
        return data1

    # 账期订单新旧程序分布情况
    def tab_new_old_account(self,data):
        data1=data.groupby(['新旧程度']).agg({'订单号':'count','下单用户':'nunique','是否逾期':'sum'}).reset_index()
        data1['逾期率']=round((data1['是否逾期']/data1['订单号']),2)
        data1=data1.sort_values(by=['订单号'],ascending=False)
        return data1

    # 当月开始发生逾期的订单
    def tab_current_month_account(self,data,start_time,end_time):
        dic1={}
        # 确定逾期开始的时间
        data1=data[data['是否逾期']==1]
        data1=data1[(data1['首次逾期时间']>=start_time)&(data1['逾期开始时间']<=end_time)]
        dic1['首次逾期订单数']=len(data1['订单号'].unique())
        data_1=pd.DataFrame(dic1,index=[0])
        # 首次发生逾期订单的用户属性分布
        data2=data1.groupby(['性别']).agg({'订单号':'count','下单用户':'nunique'}).reset_index()
        data2['订单占比']=round((data2['订单号']/data2['订单号'].sum()),2)
        data3=data1.groupby(['年龄区间']).agg({'订单号':'count','下单用户':'nunique'}).reset_index()
        data3['订单占比']=round((data3['订单号']/data3['订单号'].sum()),2)
        # 风险等级分布
        data4=data1.groupby(['风险等级']).agg({'订单号':'count','下单用户':'nunique'}).reset_index()
        data4['订单占比']=round((data4['订单号']/data4['订单号'].sum()),2)
        # 新旧程度分布
        data5=data1.groupby(['新旧程度']).agg({'订单号':'count','下单用户':'nunique'}).reset_index()
        data5['订单占比']=round((data5['订单号']/data5['订单号'].sum()),2)
        # 数据拼接
        data2=data2.rename(columns={'订单号':'订单数','下单用户':'用户数'})
        data3=data3.rename(columns={'订单号':'订单数','下单用户':'用户数'})
        data4=data4.rename(columns={'订单号':'订单数','下单用户':'用户数'})
        data5=data5.rename(columns={'订单号':'订单数','下单用户':'用户数'})
        return data_1,data2,data3,data4,data5



# 月份申请顶单数据分布情况
def current_month_data(statr_time_1,end_time_1,start_time_2,end_time_2,month):
    dic1={}
    afao=ApplyForAnOrder()
    current_month_data_zong =load_data(statr_time_1, end_time_1)
    last_month_data_zong = load_data(start_time_2, end_time_2)
    # print(data_zong)
    # 1,数据整体情况
    dic1['订单申请整体情况'] = afao.tab_zong(current_month_data_zong,last_month_data_zong)
    # 2,每天申请订单情况
    dic1['订单申请每天'] = afao.tab_everyday(current_month_data_zong)
    # 3,商户分单情况
    dic1['申请订单商户分布'] = afao.data_group_by(current_month_data_zong, '下单店铺')
    # 4,省份分单情况
    dic1['申请订单省份分布'] = afao.data_group_by(current_month_data_zong, '收货地址--省份')
    # 5,用户性别分单情况
    dic1['申请订单性别分布'] = afao.data_group_by(current_month_data_zong, '性别')
    # 6,用户年龄分单情况
    dic1['申请订单用户年龄分布'] =afao.data_group_by(current_month_data_zong, '年龄区间')
    # 7,订单月租金额区间分布情况
    dic1['订单申请月租金额分布'] =afao.data_group_by(current_month_data_zong, '月租金额区间')
    # 8,各风险等级分单情况
    dic1['申请订单风险等级分布'] = afao.data_group_by(current_month_data_zong, '风险等级',data2=last_month_data_zong,num=2)
    # 9,小程序申请订单情况
    dic1['申请订单各小程序分布'] = afao.data_group_by(current_month_data_zong, '小程序',data2=last_month_data_zong,num=2)
    # 10,商品品牌申请订单情况
    dic1['申请订单品牌分布'] = afao.data_group_by(current_month_data_zong, '品牌')
    # 11,商品机型申请订单情况
    dic1['申请订单机型分布'] = afao.data_group_by(current_month_data_zong, '机型')
    # 12，品牌--机型申请订单情况
    dic1['申请订单品牌--机型分布']= afao.tab_goods(current_month_data_zong)
    # 13,商品新旧程度申请订单情况
    dic1['申请订单新旧程度分布'] = afao.data_group_by(current_month_data_zong, '新旧程度')
    # 14,订单省份--城市分布情况
    dic1['申请订单省份--城市分布'] = afao.tab_province_city(current_month_data_zong)
    # 15,新注册用户转化情况
    dic1['申请订单新注册用户转化情况'] = afao.user_conversion(statr_time_1,end_time_1)
    # 保存数据
    save_data_1(dic1=dic1,month=month,data_name='申请订单数据')
    # print(data14)

# 账期订单数据情况
def account_data(start_time_1, end_time_1, start_time_2, end_time_2,month):
    apo=AccountingPeriodOrder()
    dic1={}
    data=load_data_overdue(start_time_1, end_time_1)
    # 1,当前月份账期订单数据
    dic1['当月账期订单数据']=apo.tab_account(data)
    # 2,账期订单用户属性分布
    dic1['账期订单用户性别'],dic1['账期订单用户年龄区间']=apo.tab_user_account(data)
    # 3,账期订单风险等级分布
    dic1['账期风险等级分布']=apo.tab_risk_level_account(data)
    # 4,账期订单省份分布
    dic1['账期订单省份']=apo.tab_province_account(data)
    # 5,账期订单省份--城市分布
    dic1['账期订单省份--城市分布']=apo.tab_province_city_account(data)
    #6,账期订单月租金额区间分布
    dic1['账期订单月租金额区间分布']=apo.tab_rent_account(data)
    #7,账期订单品牌分布
    dic1['账期订单品牌机型分布']=apo.tab_goods_account(data)
    #8,账期订单商户分布
    dic1['账期订单商户分布']=apo.tab_merchant_account(data)
    #9,账期订单新旧程度分布
    dic1['账期订单新旧程度分布']=apo.tab_new_old_account(data)
    # 当月开始发生逾期的订单
    dic1['当月首次逾期的订单数'],dic1['当月首次逾期订单性别分布'],dic1['当月首次逾期订单年龄分布'],dic1['当月首次逾期订单风险等级分布'],dic1['当月首次逾期订单新旧程度分布']=\
        apo.tab_current_month_account(data,start_time_1, end_time_1)
    # 保存数据
    save_data_1(dic1=dic1,month=month,data_name='账期订单数据')
    print(dic1)

if __name__ == '__main__':
    start_time_1 = '2023-5-1'
    end_time_1 = '2023-5-31'
    start_time_2 = '2023-4-1'
    end_time_2 = '2023-4-30'
    month='2023-5'
    current_month_data(start_time_1, end_time_1, start_time_2, end_time_2,month)
    account_data(start_time_1, end_time_1, start_time_2, end_time_2,month)
