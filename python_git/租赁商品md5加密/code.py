import hashlib

import pandas as pd

df1=pd.read_excel('./原始数据.xlsx')
list1=['租赁人名字','租赁人手机号','租赁人身份证']
for i in list1:
    df1[i]=df1[i].astype(str)
    df1[i+'加密后的']=df1[i].apply(lambda x:hashlib.md5(x.encode('utf-8')).hexdigest())

df1.to_csv('./test2.csv',index=False)
print('加密完成')