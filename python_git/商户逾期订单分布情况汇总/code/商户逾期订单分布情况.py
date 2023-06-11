# 导包
import warnings
import time
import os
from Sql.qfsql import qfsql
# from Sql_2.qfsql import qfsql
import pandas as pd
warnings.filterwarnings('ignore')
qf=qfsql()

# 数据加载类
class DataLoad():
    def data_load(self):
        sql=f"""
        select
       or1.order_number as '订单号',
       case when or1.merchant_name='易租机' then '乐购亦享' else or1.merchant_name
end as '商户名称',
       or1.user_name as '下单用户',
       or1.create_time as '下单时间',
       or1.delivery_time as '发货时间',
       case when or1.delivery_time is null then 0 else 1 end as '是否发货',
       case when us1.sex is null then '空值' else us1.sex end as '性别',
       case when us1.birthday is null then '空值' else
       timestampdiff(year,date(birthday),date(now())) end as '年龄',
       case when timestampdiff(year,date(birthday),date(now())) >0 and timestampdiff(year,date(birthday),date(now())) <=18 then '0-18'
           when timestampdiff(year,date(birthday),date(now()))>18 and timestampdiff(year,date(birthday),date(now()))<=22 then '19-22'
           when timestampdiff(year,date(birthday),date(now()))>22 and timestampdiff(year,date(birthday),date(now()))<=35 then '23-35'
           when timestampdiff(year,date(birthday),date(now()))>=36 then '36-inf'
           end as '年龄区间',
       oe1.provice as '收货地址省份',
       oe1.city as '收货地址城市',
       oe1.regoin as '收货地址区县',
       round(or1.daily_rent/100*30,2) as '每月租金',
       case when round(daily_rent/100*30,1)>0 and round(daily_rent/100*30,1)<=650 then '0-650'
           when round(daily_rent/100*30,1)>650 and round(daily_rent/100*30,1)<=1100 then '650-1100'
           when round(daily_rent/100*30,1)>1100 then '1100-inf'
       end as '月租金额区间',
       case when round(daily_rent/100*30,1)>0 and round(daily_rent/100*30,1)<=650 then 1
           when round(daily_rent/100*30,1)>650 and round(daily_rent/100*30,1)<=1100 then 2
           when round(daily_rent/100*30,1)>1100 then 3
       end as '月租金额区间_排序',
       round(og1.price/100,2) as '设备价格',
       case when round(og1.price/100,2)>0 and round(og1.price/100,2)<=4000 then '0-4000'
           when round(og1.price/100,2)>4000 and round(og1.price/100,2)<=6000 then '4000-6000'
           when round(og1.price/100,2)>6000 and round(og1.price/100,2)<=8000 then '6000-8000'
           when round(og1.price/100,2)>8000 and round(og1.price/100,2)<=10000 then '8000-10000'
           when round(og1.price/100,2)>10000 and round(og1.price/100,2)<=12000 then '10000-12000'
           when round(og1.price/100,2)>12000 and round(og1.price/100,2)<=14000 then '12000-14000'
           when round(og1.price/100,2)>14000  then '14000-inf'
         end as '设备价格区间',
       case when round(og1.price/100,2)>0 and round(og1.price/100,2)<=4000 then 1
           when round(og1.price/100,2)>4000 and round(og1.price/100,2)<=6000 then 2
           when round(og1.price/100,2)>6000 and round(og1.price/100,2)<=8000 then 3
           when round(og1.price/100,2)>8000 and round(og1.price/100,2)<=10000 then 4
           when round(og1.price/100,2)>10000 and round(og1.price/100,2)<=12000 then 5
           when round(og1.price/100,2)>12000 and round(og1.price/100,2)<=14000 then 6
           when round(og1.price/100,2)>14000  then 7
         end as '设备价格区间_排序',
       case when or1.merchant_credit_suggest='SUCCESS' then '低风险'
           when or1.merchant_credit_suggest='VALIDATE' then '中低风险'
           when or1.merchant_credit_suggest='STRICT_VALIDATE' then '中高风险'
           when or1.merchant_credit_suggest='REJECT' then '高风险'
           when or1.merchant_credit_suggest is null then '空值'
           end as '风险等级',
       case when or1.merchant_credit_suggest='SUCCESS' then 1
           when or1.merchant_credit_suggest='VALIDATE' then 2
           when or1.merchant_credit_suggest='STRICT_VALIDATE' then 3
           when or1.merchant_credit_suggest='REJECT' then 4
           when or1.merchant_credit_suggest is null then 5
           end as '风险等级_排序',
       case
           when or1.state regexp  'pending_send_goods|pending_receive_goods|running|running_overdue|returning|
                return_overdue|returned_unreceived|lease_finished|relet_finished|pending_buyout_pay|
                buyout_finished' then 1
           else 0 end as '是否成交',
       og1.goods_name as '商品名称',
       og1.model as '机型',
       og1.category as '一级分类',
       og1.type as '二级分类',
       og1.brand as '品牌',
       og1.old_level as '新旧程度',
       og1.standard as '规格',
       json_unquote(
           json_extract(og1.standard, replace(json_unquote(json_search(standard, 'all', '内存')),'scname','name'))) AS '内存',
       case when py1.是否已出账单 is null then 0
           else py1.是否已出账单 end as '是否已出账单',
       case when py1.逾期状态 is null then 0
           else py1.逾期状态 end as '是否逾期'
from `order` or1 left join (
    select
           b.order_number,
           max(b.逾期状态) as '逾期状态',
           max(b.是否已出账单) as '是否已出账单'
    from(
    select
       a.order_number,
           case when a.账期状态2='overdue_unpaid' and a.订单状态 in ('running','running_overdue') then 1 else 0 end as '逾期状态',
           a.是否已出账单
from (
select
       py1.order_number ,
       # 判断订单是否逾期
       case when py1.bill_state='waiting' then
            (case when date(py1.bill_start_time) between date_sub(date(now()),interval 3 day) and date(now())
                then 'overdue_unpaid' else 'waiting' end)
       else py1.bill_state end as '账期状态2',
         # 判断账期是否已出账单
       case when py1.bill_stage like '1/%' then
            (case when date(py1.bill_end_time)<date(now()) then 1 else 0 end)
         else 0 end as '是否已出账单',
        or1.state as '订单状态'
from payment py1 left join `order` or1 on py1.order_number=or1.order_number
where py1.bill_start_time <= date(now())
and  bill_state in ('overdue_paid','overdue_unpaid','waiting','paid')
and or1.state in ('running','running_overdue','pending_buyout_pay','buyout_finished','lease_finished')) as a) b
group by b.order_number) as py1 on or1.order_number=py1.order_number # 账期表
left join order_express oe1 on or1.order_number = oe1.order_number # 订单地址表
left join (select id,phone,sex,birthday from user where sex is not null group by phone) as us1 on or1.user_name=us1.phone #用户表
left join order_goods og1 on or1.order_number=og1.order_number # 订单商品表
where or1.order_number like 'DD%'
        """
        data=qf.data_fetcher_ji(sql)
        print('数据获取完成')
        return data

# 数据清洗类
class DataClean(object):
    def __init__(self,data,merchant_name):
        self.data=data
        self.merchant_name=merchant_name

    # 数据分组方法
    def data_group(self,data,groupby_list):
        data_1=data.groupby(groupby_list,as_index=False).agg({
                '订单号': 'count',
                '下单用户': 'nunique',
                '是否成交': 'sum',
                '是否发货': 'sum',
                '是否已出账单': 'sum',
                '是否逾期': 'sum'
            })
        data_1 = data_1.rename(columns={
            '订单号': '申请订单数',
            '下单用户': '申请用户数',
            '是否成交': '成交订单数',
            '是否发货': '发货订单数',
            '是否已出账单': '出账订单数',
            '是否逾期': '逾期订单数'
        })
        # 计算指标
        data_1['成交率'] = round((data_1['成交订单数'] / data_1['申请订单数']) * 100, 2)
        data_1['逾期率_成交'] = round((data_1['逾期订单数'] / data_1['成交订单数']) * 100, 2)
        data_1['逾期率_出账'] = round((data_1['逾期订单数'] / data_1['出账订单数']) * 100, 2)
        return data_1

    # 获取商户数据整体情况
    def get_merchant_data(self):
        dic1={}
        data_1=self.data[self.data['商户名称']==self.merchant_name].copy()
        dic1['商户名称']=self.merchant_name
        dic1['申请订单数']=data_1['订单号'].nunique()
        dic1['申请用户数']=data_1['下单用户'].nunique()
        dic1['成交订单数']=data_1[data_1['是否成交']==1]['订单号'].nunique()
        dic1['发货订单数']=data_1[data_1['是否发货']==1]['订单号'].nunique()
        dic1['出账订单数']=data_1[data_1['是否已出账单']==1]['订单号'].nunique()
        dic1['逾期订单数']=data_1[data_1['是否逾期']==1]['订单号'].nunique()
        dic1['成交率']=round((dic1['成交订单数']/dic1['申请订单数'])*100,2)
        dic1['逾期率_成交']=round((dic1['逾期订单数']/dic1['成交订单数'])*100,2)
        dic1['逾期率_出账']=round((dic1['逾期订单数']/dic1['出账订单数'])*100,2)
        data_1=pd.DataFrame(dic1,index=['0']).T
        print('商户数据整体情况获取完成')
        return data_1

    # 用户属性分布情况
    def get_user_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        # 性别分布
        data_sex=self.data_group(data1,['性别'])
        data_age=self.data_group(data1,['年龄区间'])
        data_sex_risk=self.data_group(data1,['性别','风险等级'])
        data_age_risk=self.data_group(data1,['年龄区间','风险等级'])
        print('用户属性分布情况获取完成')
        return data_sex,data_age,data_sex_risk,data_age_risk

    # 风险等级分布
    def get_risk_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_risk=self.data_group(data1,['风险等级'])
        print('风险等级分布获取完成')
        return data_risk

    # 月租金额区间分布
    def get_rent_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_rent=self.data_group(data1,['月租金额区间'])
        data_rent_risk=self.data_group(data1,['月租金额区间','风险等级'])
        print('月租金额区间分布获取完成')
        return data_rent,data_rent_risk

    # 设备价格区间分布
    def get_device_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_device=self.data_group(data1,['设备价格区间'])
        data_device_risk=self.data_group(data1,['设备价格区间','风险等级'])
        print('设备价格区间分布获取完成')
        return data_device,data_device_risk

    # 逾期订单省份分布
    def get_province_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_province=self.data_group(data1,['收货地址省份'])
        data_province_risk=self.data_group(data1,['收货地址省份','风险等级'])
        print('逾期订单省份分布获取完成')
        return data_province,data_province_risk

    # 逾期订单城市分布
    def get_city_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_city=self.data_group(data1,['收货地址省份','收货地址城市'])
        data_city_risk=self.data_group(data1,['收货地址省份','收货地址城市','风险等级'])
        print('逾期订单城市分布获取完成')
        return data_city,data_city_risk

    # 逾期订单区县分布
    def get_county_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_county=self.data_group(data1,['收货地址省份','收货地址城市','收货地址区县'])
        data_county_risk=self.data_group(data1,['收货地址省份','收货地址城市','收货地址区县','风险等级'])
        print('逾期订单区县分布获取完成')
        return data_county,data_county_risk

    # 逾期订单品牌分布
    def get_brand_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_brand=self.data_group(data1,['品牌'])
        data_brand_risk=self.data_group(data1,['品牌','风险等级'])
        print('逾期订单品牌分布获取完成')
        return data_brand,data_brand_risk

    # 逾期订单机型分布
    def get_model_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_model=self.data_group(data1,['品牌','机型'])
        data_model_risk=self.data_group(data1,['品牌','机型','风险等级'])
        print('逾期订单机型分布获取完成')
        return data_model,data_model_risk

    # 逾期订单内存分布
    def get_memory_data(self):
        data1=self.data[self.data['商户名称']==self.merchant_name].copy()
        data_memory=self.data_group(data1,['品牌','机型','内存'])
        data_memory_risk=self.data_group(data1,['品牌','机型','内存','风险等级'])
        print('逾期订单内存分布获取完成')
        return data_memory,data_memory_risk


# # 规则总结类
# class RuleSummary():
#     def __init__(self,data,merchant_name):
#         self.data=data
#         self.merchant_name=merchant_name
#
#     # 逾期订单规则总结
#     def get_overdue_rule(self):
#         # 整体数据情况



# 方法调用方法

# 数据保存方法
def save_data_1(dic1={},data_name='',merchant_name=''):
    if not os.path.exists(f'../data/{merchant_name}'):
        os.makedirs(f'../data/{merchant_name}')
    with pd.ExcelWriter(f'../data/{merchant_name}/{data_name}.xlsx') as witer:
        for k, v in dic1.items():
            v.to_excel(witer, sheet_name=k)
            print(f'{k}保存完成！！！')


def run(merchant_name):
    load_data=DataLoad()
    dic_overall_data={}
    dic_user_data={}
    dic_risk_data={}
    dic_goods_data={}
    dic_address_data={}
    data=load_data.data_load()
    data_clean=DataClean(data,merchant_name)
    # 数据整体情况
    merchant_overall_data=data_clean.get_merchant_data()
    dic_overall_data['逾期整体情况']=merchant_overall_data
    # 用户属性风险等级分布情况
    user_sex_data,user_age_data,user_sex_data_risk,user_age_data_risk=data_clean.get_user_data()
    dic_user_data['用户性别分布']=user_sex_data
    dic_user_data['用户年龄阶段分布']=user_age_data
    dic_user_data['用户性别_风险等级分布']=user_sex_data_risk
    dic_user_data['用户年龄阶段_风险等级分布']=user_age_data_risk
    # 风险等级分布情况
    risk_data=data_clean.get_risk_data()
    dic_risk_data['风险等级分布']=risk_data
    # 月租金额区间分布情况
    rent_data,rent_data_risk=data_clean.get_rent_data()
    dic_goods_data['月租金额区间分布']=rent_data
    dic_goods_data['月租金额区间_风险等级分布']=rent_data_risk
    # 设备价格区间分布情况
    device_data,device_data_risk=data_clean.get_device_data()
    dic_goods_data['设备价格区间分布']=device_data
    dic_goods_data['设备价格区间_风险等级分布']=device_data_risk
    # 逾期订单省份分布情况
    province_data,province_data_risk=data_clean.get_province_data()
    dic_address_data['逾期订单省份分布']=province_data
    dic_address_data['逾期订单省份_风险等级分布']=province_data_risk
    # 逾期订单城市分布情况
    city_data,city_data_risk=data_clean.get_city_data()
    dic_address_data['逾期订单城市分布']=city_data
    dic_address_data['逾期订单城市_风险等级分布']=city_data_risk
    # 逾期订单区县分布情况
    county_data,county_data_risk=data_clean.get_county_data()
    dic_address_data['逾期订单区县分布']=county_data
    dic_address_data['逾期订单区县_风险等级分布']=county_data_risk
    # 逾期订单品牌分布情况
    brand_data,brand_data_risk=data_clean.get_brand_data()
    dic_goods_data['逾期订单品牌分布']=brand_data
    dic_goods_data['逾期订单品牌_风险等级分布']=brand_data_risk
    # 逾期订单机型分布情况
    model_data,model_data_risk=data_clean.get_model_data()
    dic_goods_data['逾期订单机型分布']=model_data
    dic_goods_data['逾期订单机型_风险等级分布']=model_data_risk
    # 逾期订单内存分布情况
    memory_data,memory_data_risk=data_clean.get_memory_data()
    dic_goods_data['逾期订单内存分布']=memory_data
    dic_goods_data['逾期订单内存_风险等级分布']=memory_data_risk
    # 数据保存
    save_data_1(dic1=dic_overall_data,data_name='逾期整体情况',merchant_name=merchant_name)
    save_data_1(dic1=dic_user_data,data_name='用户属性分布',merchant_name=merchant_name)
    save_data_1(dic1=dic_risk_data,data_name='风险等级分布',merchant_name=merchant_name)
    save_data_1(dic1=dic_goods_data,data_name='商品属性分布',merchant_name=merchant_name)
    save_data_1(dic1=dic_address_data,data_name='地址属性分布',merchant_name=merchant_name)
    print('数据保存完成！！！')


if __name__ == '__main__':
    run('星海租赁')
    run('四川星皓未来科技有限公司')
    run('咖租机')