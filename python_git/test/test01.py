from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import datetime

# X = [[0, 'a'], [0, 'b'], [1, 'a'], [2, 'b']]
#
# ct = ColumnTransformer(
#     [('one_hot_encoder', OneHotEncoder(categories='auto'), [0])],   # The column numbers to be transformed (here is [0] but can be [0, 1, 3])
#     remainder='passthrough'                                         # Leave the rest of the columns untouched
# )
#
# X = ct.fit_transform(X)
# print(X)

#获取当前时间
time_now=datetime.datetime.now().strftime('%Y-%m-%d')
print(time_now)
print(datetime.datetime.now()-datetime.timedelta(days=7))
print(datetime.datetime.now()-datetime.timedelta(days=1))
# 判断今天是否是周一
if datetime.datetime.now().weekday() == 0:
    print('今天是周一')

# 获取今天是周几
print(datetime.datetime.now().weekday())

# 获取每月的最后一天
print(datetime.datetime.now().replace(day=1, month=datetime.datetime.now().month + 1) - datetime.timedelta(days=1))

# 获取上个月的第一天
print(datetime.datetime.now().replace(day=1, month=datetime.datetime.now().month - 1))

# 获取今天为当月第几周
print(datetime.datetime.now().isocalendar()[1])

# 获取上一年的这个月的第一天
print(datetime.datetime.today().replace(day=1, month=datetime.datetime.now().month, year=datetime.datetime.now().year - 1))
print(datetime.datetime.today()-datetime.timedelta(days=56))



import datetime

def get_week_number(date_string):
    date = datetime.datetime.strptime(date_string, "%Y/%m/%d")
    start_date = datetime.datetime.strptime("2023/5/3", "%Y/%m/%d")
    print((date - start_date).days)
    week_number = (date - start_date).days // 7 + 1
    return week_number

date_string = "2023/6/28"
week_number = get_week_number(date_string)
print(f"{date_string}所属的周数为：{week_number}")
