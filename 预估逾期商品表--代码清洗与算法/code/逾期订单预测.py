# 导入需要的库
import time
import warnings

import joblib as jl
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings('ignore')

dic1={}

# 加载数据集
def load_data(path):
    # 加载数据集
    try:
        data=pd.read_csv(path,encoding='gbk')
    except:
        data=pd.read_csv(path,encoding='utf-8')
    # 转化为DataFrame格式
    data_df = pd.DataFrame(data)
    print('数据集大小：',data_df.shape)
    print('数据集特征：',data_df.columns)
    return data_df

# 特征工程方法--训练集
def feature_engineering_train(data_df):
    #选取特征
    X=data_df.iloc[:,:-5]
    # 对成交省份进行one-hot编码
    X = pd.get_dummies(X, columns=['下单店铺','成交订单省份top1'])
    X['low_mid_low']=data_df['低风险逾期率']/data_df['中低风险逾期率']
    X['low_mid_high']=data_df['低风险逾期率']/data_df['中高风险逾期率']
    X['mid_low_mid_high']=data_df['中低风险逾期率']/data_df['中高风险逾期率']
    X.fillna(0,inplace=True)
    X.replace(np.inf,0,inplace=True)
    list1 = ['成交用户数', '成交订单数', '成交订单平均年龄', '成交订单月租平均金额']
    # 进行数据归一化处理
    mm = MinMaxScaler()
    # mm=StandardScaler()
    X[list1] = mm.fit_transform(X[list1])
    print('特征工程后数据集大小：', X.shape)
    return X

# 特征工程方法--测试集
def feature_engineering_test(data_df):
    #选取特征
    X=data_df.iloc[:,:-1]
    # 对成交省份进行one-hot编码
    X = pd.get_dummies(X, columns=['下单店铺','成交订单省份top1'])
    X.fillna(0,inplace=True)
    X.replace(np.inf,0,inplace=True)
    list1 = ['成交用户数', '成交订单数', '成交订单平均年龄', '成交订单月租平均金额']
    # 进行数据归一化处理
    mm = MinMaxScaler()
    # mm=StandardScaler()
    X[list1] = mm.fit_transform(X[list1])
    print('特征工程后数据集大小：', X.shape)
    return X


# 双模型预测 对结果进行加权
def model_train_1(X_train,y_train,tager):
    print(f'开始训练模型：{tager}')
    # 参数调优
    params = {
        'max_depth': [3, 5, 7],
        'n_estimators': [50, 100, 150]
    }

    param_grid = {
        'n_estimators': [50, 100, 150],
        'learning_rate': [0.01, 0.05, 0.1],
        'max_depth': [3, 5, 8],
        'min_samples_split': [2, 4, 6],
        'min_samples_leaf': [1, 2, 3],
        'subsample': [0.5, 0.8, 1],
        'max_features': ['auto', 'sqrt', 'log2']
    }
    # 建立双模型进行数据预测
    rf = RandomForestRegressor(random_state=42)
    gbdt = GradientBoostingRegressor(random_state=42)
    # 建立GridSearchCV对象
    grid_search_rf = GridSearchCV(rf, param_grid=params, cv=5, n_jobs=-1)
    grid_search_gbdt = GridSearchCV(gbdt, param_grid=param_grid, cv=5, n_jobs=-1)
    # 模型训练
    grid_search_rf.fit(X_train, y_train)
    grid_search_gbdt.fit(X_train, y_train)
    # 输出最佳参数
    print("Best parameters_rf:", grid_search_rf.best_params_)
    print("Best parameters_gbdt:", grid_search_gbdt.best_params_)
    # 输出最佳模型评分
    best_model_rf = grid_search_rf.best_estimator_
    best_model_gbdt = grid_search_gbdt.best_estimator_
    return best_model_rf,best_model_gbdt

# 单模型预测
def model_train_2(X_train,y_train,tager):
    print(f'开始训练模型：{tager}')
    params = {
        'max_depth': [3, 5, 7],
        'n_estimators': [50, 100, 150],
        'min_samples_split': [2, 4, 6],
        'min_samples_leaf': [1, 2, 3],
        'bootstrap': [True, False]
    }
    rf = RandomForestRegressor(random_state=42)
    # 建立GridSearchCV对象
    grid_search = GridSearchCV(rf, param_grid=params, cv=5, n_jobs=-1)
    # 模型训练
    grid_search.fit(X_train, y_train)
    # 输出最佳参数
    print("Best parameters:", grid_search.best_params_)
    # 输出最佳模型评分
    best_model = grid_search.best_estimator_
    return best_model


def model_predict(X_test,tager,model1,model2='',y_test=''):
    if model2!='':
        y_pred_model1 = model1.predict(X_test)
        y_pred_model2 = model1.predict(X_test)
        # 对预测者进行加权
        y_pred = 0.8 * y_pred_model1 + 0.2 * y_pred_model2
        dic1[f'{tager}_预测'] = np.around(y_pred, 3)
        if 1==1:
            if y_test!='':
                print(y_test)
                print(np.around(y_pred, 3))
                print(f"Best model R2 score: {r2_score(y_test, y_pred)}")
                print(f"Best model RMSE score: {np.sqrt(mean_squared_error(y_test, y_pred))}")
                print('------------------')
    else:
        y_pred_model1 = model1.predict(X_test)
        dic1[f'{tager}_预测'] = np.around(y_pred_model1, 3)
        if 1==1:
            if y_test!='':
                print(y_test)
                print(np.around(y_pred_model1, 3))
                print(f"Best model R2 score: {r2_score(y_test, y_pred_model1):.4f}")
                print(f"Best model RMSE score: {np.sqrt(mean_squared_error(y_test, y_pred_model1)):.4f}")

# 模型训练与保存--总的调用函数
def model_train_zong(X_train,y_low_train,y_mid_low_train,y_mid_high_train,y_high_train):
    # 调用模型进行训练
    model1_low, model2_low = model_train_1(X_train, y_low_train, '低风险')
    model1_mid_low, model2_mid_low = model_train_1(X_train, y_mid_low_train, '中低风险')
    model1_mid_high, model2_mid_high = model_train_1(X_train, y_mid_high_train, '中高风险')
    model1_high= model_train_2(X_train, y_high_train, '高风险')
    # 保存模型
    jl.dump(model1_low, '../data/model/model1_low.pkl')
    jl.dump(model2_low, '../data/model/model2_low.pkl')
    jl.dump(model1_mid_low, '../data/model/model1_mid_low.pkl')
    jl.dump(model2_mid_low, '../data/model/model2_mid_low.pkl')
    jl.dump(model1_mid_high, '../data/model/model1_mid_high.pkl')
    jl.dump(model2_mid_high, '../data/model/model2_mid_high.pkl')
    jl.dump(model1_high, '../data/model/model1_high.pkl')

# 模型预测与加载--总的调用函数
def model_predict_zong(X_test,y_low_test='',y_mid_low_test='',y_mid_high_test='',y_high_test=''):
    # 加载模型
    model1_low = jl.load('../data/model/model1_low.pkl')
    model2_low = jl.load('../data/model/model2_low.pkl')
    model1_mid_low = jl.load('../data/model/model1_mid_low.pkl')
    model2_mid_low = jl.load('../data/model/model2_mid_low.pkl')
    model1_mid_high = jl.load('../data/model/model1_mid_high.pkl')
    model2_mid_high = jl.load('../data/model/model2_mid_high.pkl')
    model1_high = jl.load('../data/model/model1_high.pkl')
    # 进行预测
    model_predict(X_test, '低风险', model1_low, model2=model2_low,y_test= y_low_test,)
    model_predict(X_test, '中低风险', model1_mid_low, model2=model2_mid_low,y_test= y_mid_low_test,)
    model_predict(X_test,'中高风险', model1_mid_high, model2=model2_mid_high,y_test= y_mid_high_test,)
    model_predict(X_test,'高风险', model1_high, y_test= y_high_test,)

#主函数
if __name__ == '__main__':
    time_start = time.time()
    path='../data/data_clearn.csv'
    data_df=load_data(path)
    X=feature_engineering_train(data_df)
    y_low=data_df['低风险逾期率'].values
    y_mid_low=data_df['中低风险逾期率'].values
    y_mid_high=data_df['中高风险逾期率'].values
    y_high=data_df['高风险逾期率'].values
    #划分训练集和测试集
    X_train,X_test,y_low_train,y_low_test,y_mid_low_train,y_mid_low_test,y_mid_high_train,y_mid_high_test,y_high_train,y_high_test=\
        train_test_split(X,y_low,y_mid_low,y_mid_high,y_high,test_size=0.1,random_state=42)
    #模型训练
    model_train_zong(X_train,y_low_train,y_mid_low_train,y_mid_high_train,y_high_train)
    #模型预测
    model_predict_zong(X_test,y_low_test=y_low_test,y_mid_low_test=y_mid_low_test,y_mid_high_test=y_mid_high_test,y_high_test=y_high_test)
    df1=pd.DataFrame(dic1)
    df1[df1<0]=0
    print(df1)
    time_end = time.time()
    # 计算程序执行所需时间
    print("程序运行时间：%.2f s" % (time_end - time_start))