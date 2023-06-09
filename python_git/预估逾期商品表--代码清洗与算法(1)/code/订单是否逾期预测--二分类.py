import pandas as pd
import numpy as np
import warnings
import joblib as jl
from sklearn.preprocessing import MinMaxScaler,StandardScaler,OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
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
def feature_engineering_train(data_df,type='train'):
    if type=='train':
        data_df = data_df[['年龄', '性别', '每月租金', '设备价格', '押金类型',
                           '碎屏保险', '是否逾期','收货地址--省份','风险等级','渠道','新旧程度'
                           ]].copy()
    elif type=='test':
        data_df = data_df[['年龄', '性别', '每月租金', '设备价格', '押金类型',
                           '碎屏保险','收货地址--省份','风险等级','渠道','新旧程度'
                           ]].copy()
    else:
        return 'num参数输入错误'
    data_df.dropna(inplace=True)
    # 对数类别数据进行映射
    data_df['性别'] = data_df['性别'].map(get_map_dict(data_df, '性别'))
    data_df['押金类型'] = data_df['押金类型'].map(get_map_dict(data_df, '押金类型'))
    # data_df['是否优惠'] = data_df['是否优惠'].map(get_map_dict(data_df, '是否优惠'))
    data_df['风险等级'] = data_df['风险等级'].map(get_map_dict(data_df, '风险等级'))
    data_df['新旧程度'] = data_df['新旧程度'].map(get_map_dict(data_df, '新旧程度'))
    data_df['渠道'] = data_df['渠道'].map(get_map_dict(data_df, '渠道'))
    # 对数据进行分箱处理
    age_bins = [0, 20, 30, 40, 50, 60, 70, 80]
    age_group_names = ['0-20', '21-30', '31-40', '41-50', '51-60', '61-70', '71-80']
    data_df['年龄_cut'] = pd.cut(data_df['年龄'], age_bins, labels=age_group_names)
    equip_amt_bins = [0, 1000, 4000, 7000, 10000, 15000]
    equip_amt_labels = ['低值', '中低值', '中值', '中高值', '高值']
    data_df['设备价格_cut'] = pd.cut(data_df['设备价格'], equip_amt_bins, labels=equip_amt_labels)
    broken_screen_bin = [0, 100, 200, 300, 400, 500, 600]
    broken_screen_group_names = ['0-100', '100-200', '200-300', '300-400', '400-500', '500-600']
    data_df['碎屏险金额_cut'] = pd.cut(data_df['碎屏保险'], broken_screen_bin, labels=broken_screen_group_names)
    # 对分箱后的数据进行映射
    data_df['年龄_cut'] = data_df['年龄_cut'].map(get_map_dict2(age_group_names))
    data_df['设备价格_cut'] = data_df['设备价格_cut'].map(get_map_dict2(equip_amt_labels))
    data_df['碎屏险金额_cut'] = data_df['碎屏险金额_cut'].map(get_map_dict2(broken_screen_group_names))
    # 衍生数据特征 对数据进行对数处理
    # data_df['月租金额_outlier']=(data_df['月租金额']>1.5*data_df['月租金额'].mean()).astype(int)
    # data_df['设备价格_outlier']=(data_df['设备价格']>1.5*data_df['设备价格'].mean()).astype(int)
    # data_df.dropna(inplace=True)
    data_df.fillna(0,inplace=True)
    # 对数据进行归一化处理
    scaler = StandardScaler()
    list1 = ['年龄', '每月租金', '设备价格']
    for i in list1:
        data_df[i + '_log'] = np.log(data_df[i])
        data_df[i+'_log']=scaler.fit_transform(data_df[[i+'_log']])
        data_df[i]=scaler.fit_transform(data_df[[i]])
    print('特征处理之后的数据集大小：', data_df.shape)
    print('特征处理之后数据集特征：', data_df.columns)
    return data_df


def get_data_one_hot(data_df_train,data_test,data_df_zong):
    if 0==1:
        list1=[]
        list2=[]
        for i in data_df_train.columns:
            train_categories = data_df_train[i].tolist()
            list1.append(train_categories)
            test_categories = data_test[i].tolist()
            list1.append(test_categories)
        for j in list1:
            for k in j:
                list2.append(k)
        n_categories=len(set(list2))
        print(n_categories)
    enc=OneHotEncoder()
    enc.fit(data_df_zong)
    feature_names=enc.get_feature_names_out(data_df_train.columns)
    data_df_train_one_hot=pd.DataFrame(enc.transform(data_df_train).toarray(),columns=feature_names)
    data_df_test_one_hot=pd.DataFrame(enc.transform(data_test).toarray(),columns=feature_names)
    return data_df_train_one_hot,data_df_test_one_hot


# 开始训练模型
def train_model(data_df):
    data_df.dropna(inplace=True)
    # 对数据进行切分
    n = data_df[data_df['是否逾期'] == 1].shape[0]
    data_df_2=data_df[data_df['是否逾期']==0].sample(n=n).append(data_df[data_df['是否逾期']==1])
    X=data_df_2.drop(['是否逾期'],axis=1)
    y=data_df_2['是否逾期']
    print(y.value_counts())
    x_train,x_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 逻辑回归
    lr = LogisticRegression()
    param_grid_lr = {
        'C': [0.1, 0.5, 1],
        'penalty': ['l1', 'l2'],
        'solver': ['newton-cg', 'lbfgs']
    }
    grid_search_lr = GridSearchCV(lr, param_grid_lr, cv=5)
    grid_search_lr.fit(x_train, y_train)
    print('逻辑回归最优参数：', grid_search_lr.best_params_)
    # print('逻辑回归最优分数：', grid_search_lr.best_score_)
    lr=LogisticRegression(C=grid_search_lr.best_params_['C'],penalty=grid_search_lr.best_params_['penalty'],solver=grid_search_lr.best_params_['solver'])
    lr.fit(x_train, y_train)
    y_pred_lr = lr.predict(x_test)
    print('逻辑回归准确率：', accuracy_score(y_test, y_pred_lr))
    print('逻辑回归召回率：', recall_score(y_test, y_pred_lr))
    print('逻辑回归精确率：', precision_score(y_test, y_pred_lr))
    print('逻辑回归F1-score：', f1_score(y_test, y_pred_lr))
    lr_f1_score = f1_score(y_test, y_pred_lr)
    print('---------------------------------------------')
    # 随机森林
    rf = RandomForestClassifier()
    # 进行网格搜索
    param_grid = {'n_estimators': [100, 200, 300, 400, 500],
                    'max_depth': [5, 10, 15, 20, 25]}
    grid_search = GridSearchCV(rf, param_grid, cv=5,n_jobs=-1)
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
    rf_f1_score=f1_score(y_test, y_pred_rf)
    print('混淆矩阵:', confusion_matrix(y_test, y_pred_rf))
    print('---------------------------------------------')
    y_pred_rf = rf.predict_proba(x_test)
    y_pred_lr = lr.predict_proba(x_test)
    y_pred = (y_pred_rf[:, 1] * 0.6 + y_pred_lr[:, 1] * 0.4) > 0.5
    print('加权平衡后的准确率：', accuracy_score(y_test, y_pred))
    print('加权平衡后的召回率：', recall_score(y_test, y_pred))
    print('加权平衡后的精确率：', precision_score(y_test, y_pred))
    print('加权平衡后的F1-score：', f1_score(y_test, y_pred))
    print('---------------------------------------------')
    print('混淆矩阵:', confusion_matrix(y_test, y_pred))
    return rf,lr,f1_score(y_test, y_pred)


# 预测
def predict(data_df):
    # 加载模型
    rf = jl.load('../model/rf.pkl')
    lr = jl.load('../model/lr.pkl')
    data_df.dropna(inplace=True)
    print(f'测试数据集维度{data_df.shape}')
    # 预测
    y_pred_rf = rf.predict_proba(data_df)
    y_pred_lr = lr.predict_proba(data_df)
    y_pred = (y_pred_rf[:, 1] * 0.6 + y_pred_lr[:, 1] * 0.4) > 0.5
    return y_pred




if __name__ == '__main__':
    path_train='../data_2/训练数据集.xlsx'
    path_test='../data_2/测试数据集.xlsx'
    path_zong='../data_2/总的数据集.xlsx'
    data_df_train = load_data(path_train)
    data_df_test = load_data(path_test)
    data_df_zong = load_data(path_zong)
    # data_df_test=data_df_train.sample(n=500).copy()
    # data_df_test=data_df_test.drop(columns=['逾期次数','逾期天数'])
    # data_df_test.to_csv('../data/预测结果/测试数据.csv',index=False)
    data_df_train_feature = feature_engineering_train(data_df_train,type='train')
    data_df_test_feature = feature_engineering_train(data_df_test,type='test')
   # 对数据进行独热编码处理
    list2 = ['收货地址--省份']
    data_df_train_one_hot = data_df_train_feature[list2].copy()
    data_df_test_one_hot = data_df_test_feature[list2].copy()
    data_df_one_hot_zong=data_df_zong[list2].copy()
    # data_one_hot_train,data_one_hot_test = get_data_one_hot(data_df_train_one_hot,data_df_test_one_hot)
    data_one_hot_train,data_one_hot_test = get_data_one_hot(data_df_train_one_hot,data_df_test_one_hot,data_df_one_hot_zong)
    data_df_train_feature.drop(columns=list2,inplace=True)
    data_df_test_feature.drop(columns=list2,inplace=True)
    data_df_train_feature.to_excel('../data/feature_engineering_dataset/训练数据集特征工程.xlsx',index=False)
    data_df_test_feature.to_excel('../data/feature_engineering_dataset/测试数据集特征工程.xlsx',index=False)
    data_one_hot_train.to_excel('../data/feature_engineering_dataset/训练数据集独热编码.xlsx',index=False)
    data_one_hot_test.to_excel('../data/feature_engineering_dataset/测试数据集独热编码.xlsx',index=False)

    data_df_train_feature_1=pd.read_excel('../data/feature_engineering_dataset/训练数据集特征工程.xlsx')
    data_df_test_feature_1=pd.read_excel('../data/feature_engineering_dataset/测试数据集特征工程.xlsx')
    data_one_hot_train_1=pd.read_excel('../data/feature_engineering_dataset/训练数据集独热编码.xlsx')
    data_one_hot_test_1=pd.read_excel('../data/feature_engineering_dataset/测试数据集独热编码.xlsx')
    data_df_train_1 = pd.concat([data_df_train_feature_1,data_one_hot_train_1],axis=1)
    data_df_test_1 = pd.concat([data_df_test_feature_1,data_one_hot_test_1],axis=1)
    # 模型训练
    best_f1_lr=0
    best_f1_rf=0
    for i in range(5):
        print('第{}次训练'.format(i+1))
        rf,lr,f1 = train_model(data_df_train_1)
        if f1>best_f1_lr:
            best_f1_lr=f1
            jl.dump(lr, '../model/lr.pkl')
            jl.dump(rf, '../model/rf.pkl')

    # train_model(data_df_train)
    # 模型预测
    print('.....',data_df_test.dropna(subset=['年龄', '性别', '每月租金', '设备价格', '押金类型',
                           '碎屏保险','收货地址--省份','风险等级','渠道','新旧程度'
                           ]).shape)
    data_df_test.dropna(subset=['年龄', '性别', '每月租金', '设备价格', '押金类型',
                           '碎屏保险','收货地址--省份','风险等级','渠道','新旧程度'
                           ],inplace=True)
    y_pred=predict(data_df_test_1)
    # print(data_df_test.dropna()['是否逾期'].sum())
    print(len(y_pred))
    print(y_pred.sum())
    data_df_test['是否逾期_1']=y_pred.astype(int)
    data_df_test.to_excel('../data/预测结果/预测结果-2.xlsx',index=False)
    print('预测结果保存成功！')


