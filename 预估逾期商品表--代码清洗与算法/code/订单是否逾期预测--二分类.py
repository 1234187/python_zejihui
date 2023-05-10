import pandas as pd
import numpy as np
import warnings
from sklearn.preprocessing import MinMaxScaler,StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import accuracy_score, recall_score, precision_score, f1_score, confusion_matrix, roc_auc_score
from sklearn.model_selection import KFold, cross_val_score, StratifiedKFold

warnings.filterwarnings('ignore')

def load_data(path,encoding=''):
    data = pd.read_excel(path)
    print('数据集大小：',data.shape)
    print('数据集特征：',data.columns)
    return data


# 获取映射字典
def get_map_dict(data,field):
    dic1={}
    num=0
    for i in data[field].unique():
        dic1[i]=num
        num+=1
    return dic1

# 分箱后的数据进行排序编号
def get_map_dict2(field):
    return {j:i for i,j in enumerate(field)}

# 进行数据特征工程处理
def feature_engineering_train(data_df):
    # 选取特征
    data_df = data_df[['下单店铺', '年龄', '性别', '收货地址省份', '月租金额', '设备价格', '是否免押', '是否优惠', '下单渠道',
                       '碎屏险金额', '风险等级', '新旧程度', '下单机型', '是否逾期'
                    ]].copy()
    data_df.dropna(inplace=True)
    # 对数类别数据进行映射
    data_df['性别'] = data_df['性别'].map(get_map_dict(data_df, '性别'))
    data_df['是否免押'] = data_df['是否免押'].map(get_map_dict(data_df, '是否免押'))
    data_df['是否优惠'] = data_df['是否优惠'].map(get_map_dict(data_df, '是否优惠'))
    data_df['风险等级'] = data_df['风险等级'].map(get_map_dict(data_df, '风险等级'))
    data_df['新旧程度'] = data_df['新旧程度'].map(get_map_dict(data_df, '新旧程度'))
    data_df['是否逾期'] = data_df['是否逾期'].map(get_map_dict(data_df, '是否逾期'))
    # 对数据进行分箱处理
    age_bins = [0, 20, 30, 40, 50, 60, 70, 80]
    age_group_names = ['0-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80']
    data_df['年龄_cut'] = pd.cut(data_df['年龄'], age_bins, labels=age_group_names)
    equip_amt_bins = [0, 1000, 4000, 7000, 10000, 15000]
    equip_amt_labels = ['低值', '中低值', '中值', '中高值', '高值']
    data_df['设备价格_cut'] = pd.cut(data_df['设备价格'], equip_amt_bins, labels=equip_amt_labels)
    broken_screen_bin = [0, 100, 200, 300, 400, 500, 600]
    broken_screen_group_names = ['0-100', '100-200', '200-300', '300-400', '400-500', '500-600']
    data_df['碎屏险金额_cut'] = pd.cut(data_df['碎屏险金额'], broken_screen_bin, labels=broken_screen_group_names)
    # 对分箱后的数据进行映射
    data_df['年龄_cut'] = data_df['年龄_cut'].map(get_map_dict2(age_group_names))
    data_df['设备价格_cut'] = data_df['设备价格_cut'].map(get_map_dict2(equip_amt_labels))
    data_df['碎屏险金额_cut'] = data_df['碎屏险金额_cut'].map(get_map_dict2(broken_screen_group_names))
    # 衍生数据特征 对数据进行对数处理
    # data_df['月租金额_outlier']=(data_df['月租金额']>1.5*data_df['月租金额'].mean()).astype(int)
    # data_df['设备价格_outlier']=(data_df['设备价格']>1.5*data_df['设备价格'].mean()).astype(int)
    data_df.dropna(inplace=True)
    # 对数据进行归一化处理
    scaler = StandardScaler()

    list1 = ['年龄', '月租金额', '设备价格']
    for i in list1:
        data_df[i + '_log'] = np.log(data_df[i])
        data_df[i+'_log']=scaler.fit_transform(data_df[[i+'_log']])
        data_df[i]=scaler.fit_transform(data_df[[i]])
    print('特征处理之后的数据集大小：', data_df.shape)
    print('特征处理之后数据集特征：', data_df.columns)
    # 对数据进行one-hot编码
    data_df = pd.get_dummies(data_df,
                             columns=['下单店铺', '收货地址省份', '下单渠道', '下单机型'])
    return data_df

# 开始训练模型
def train_model(data_df):
    n = data_df[data_df['是否逾期'] == 1].shape[0]
    data_df_2=data_df[data_df['是否逾期']==0].sample(n=n).append(data_df[data_df['是否逾期']==1])
    X=data_df_2.drop(['是否逾期'],axis=1)
    y=data_df_2['是否逾期']
    print(y.value_counts())
    x_train,x_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)

    # 逻辑回归
    lr = LogisticRegression()
    lr.fit(x_train, y_train)
    y_pred_lr = lr.predict(x_test)
    print('逻辑回归准确率：', accuracy_score(y_test, y_pred_lr))
    print('逻辑回归召回率：', recall_score(y_test, y_pred_lr))
    print('逻辑回归精确率：', precision_score(y_test, y_pred_lr))
    print('逻辑回归F1-score：', f1_score(y_test, y_pred_lr))
    print('---------------------------------------------')
    # 随机森林
    rf = RandomForestClassifier()
    # 进行网格搜索
    param_grid = {'n_estimators': [100, 200, 300, 400, 500],
                    'max_depth': [5, 10, 15, 20, 25]}
    grid_search = GridSearchCV(rf, param_grid, cv=5)
    grid_search.fit(x_train, y_train)
    print('随机森林最优参数：', grid_search.best_params_)
    rf = RandomForestClassifier(n_estimators=grid_search.best_params_['n_estimators'],
                                max_depth=grid_search.best_params_['max_depth'])
    rf.fit(x_train, y_train)
    y_pred_rf = rf.predict(x_test)
    print('随机森林准确率：', accuracy_score(y_test, y_pred_rf))
    print('随机森林召回率：', recall_score(y_test, y_pred_rf))
    print('随机森林精确率：', precision_score(y_test, y_pred_rf))
    print('随机森林F1-score：', f1_score(y_test, y_pred_rf))
    print('混淆矩阵:', confusion_matrix(y_test, y_pred_rf))
    print('---------------------------------------------')
    y_pred_rf = rf.predict_proba(x_test)
    y_pred_lr = lr.predict_proba(x_test)
    y_pred = (y_pred_rf[:, 1] * 0.6 + y_pred_lr[:, 1] * 0.2) > 0.5
    print('加权平衡后的准确率：', accuracy_score(y_test, y_pred))
    print('加权平衡后的召回率：', recall_score(y_test, y_pred))
    print('加权平衡后的精确率：', precision_score(y_test, y_pred))
    print('加权平衡后的F1-score：', f1_score(y_test, y_pred))
    print('---------------------------------------------')
    print('混淆矩阵:', confusion_matrix(y_test, y_pred))


if __name__ == '__main__':
    path='../data/训练原始数据.xlsx'
    data_df = load_data(path)
    data_df=feature_engineering_train(data_df)
    train_model(data_df)



