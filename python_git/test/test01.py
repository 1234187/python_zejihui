from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import datetime

X = [[0, 'a'], [0, 'b'], [1, 'a'], [2, 'b']]

ct = ColumnTransformer(
    [('one_hot_encoder', OneHotEncoder(categories='auto'), [0])],   # The column numbers to be transformed (here is [0] but can be [0, 1, 3])
    remainder='passthrough'                                         # Leave the rest of the columns untouched
)

X = ct.fit_transform(X)
print(X)

#获取当前时间
time_now=datetime.datetime.now().strftime('%Y-%m-%d')
print(time_now)