import pandas as pd
import numpy as np
import datetime
import warnings
from sqlalchemy import create_engine
from Sql.qfsql import qfsql
qf=qfsql()
warnings.filterwarnings('ignore')

def load_data(path):
    if path.endswith('.csv'):
        data1=pd.read_csv(path)
    elif path.endswith('.xlsx'):
        data1=pd.read_excel(path)
    return data1

def write_data(data,columns_name,coon):
    data.columns = columns_name
    data.to_sql('channel_costs', con=coon, if_exists='append', index=False)
    print('数据存储完成')


if __name__ == '__main__':
    path='../data/cost_data/已外部合作渠道.xlsx'
    cost_data=load_data()
    columns_name = ['channel_name', 'flow_type', 'cooperation_mode', 'unit_price', 'settlement_period', 'invoice',
                    'create_time']
    cost_data = cost_data.drop(columns='编号', axis=1)
    cost_data['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d')
    write_data(cost_data, columns_name)