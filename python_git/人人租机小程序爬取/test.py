import pandas as pd
data1=pd.read_csv('./人人租机商家数据.csv',encoding='utf-8-sig')
len_1=len(data1['商家名称'].unique())

# for i in range(1,43):
#     for j in range(i,i*10+1):
#         if j>len_1:
#             continue
#         print(j)
#         print(data1['商家名称'].unique()[j-1])
#
# i=1
# while i<len_1:
#     for j in range(i,i*10+1):
#         # if j>len_1:
#         #     continue
#         print(j)
#         # print(data1['商家名称'].unique()[j-1])
#     i=i*10+1


for i in range(0,int(round(len_1/10,0))+1):
    for j in range(0,10):
        if i*10+j>=len_1:
            break
        print(i*10+j)
        print(data1['商家名称'].unique()[i*10+j])

# print(data1['商家名称'].unique())