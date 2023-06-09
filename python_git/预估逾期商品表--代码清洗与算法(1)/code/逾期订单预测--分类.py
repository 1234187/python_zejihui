# 导入需要的库
import pandas as pd
import numpy as np
import warnings
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.preprocessing import StandardScaler,MinMaxScaler
warnings.filterwarnings('ignore')

# 加载数据集
data=pd.read_csv('../data/data_clearn.csv',encoding='gbk')
# 转化为DataFrame格式
data_df = pd.DataFrame(data)

# 特征工程
#选取特征
X=data_df.iloc[:,:-5]
# print(X.columns)
# 对成交省份进行one-hot编码
X = pd.get_dummies(X, columns=['下单店铺','成交订单省份top1'])
list1 = ['成交用户数', '成交订单数', '成交订单平均年龄', '成交订单月租平均金额']

# 进行数据归一化处理
mm = MinMaxScaler()
X[list1] = mm.fit_transform(X[list1])
dic1={}
list_1=['低风险逾期率','中低风险逾期率','中高风险逾期率','高风险逾期率']
for i in list_1:
    y=data_df[i].values
    #划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, random_state=42)
    if 0==1:
        # 建立模型列表
        models = [
            LinearRegression(), #线性回归
            Ridge(), #岭回归
            Lasso(), #Lasso回归
            ElasticNet(), #弹性网络回归
            RandomForestRegressor(), #随机森林回归
            GradientBoostingRegressor() #梯度提升回归
        ]
        # 建立模型评分列表
        r2_scores = []
        rmse_scores = []
        # 模型训练及评估
        for model in models:
            # 训练模型
            model.fit(X_train, y_train)
            # 预测
            y_pred = model.predict(X_test)
            # 评估
            r2_scores.append(r2_score(y_test, y_pred))
            rmse_scores.append(np.sqrt(mean_squared_error(y_test, y_pred)))
        # 输出评分
        for i in range(len(models)):
            print(type(models[i]).__name__)
            print('R2 score: ', r2_scores[i])
            print('RMSE score: ', rmse_scores[i])
            print('------------------')


    # 参数调优
    # params = {
    #     'n_estimators': [150,200, 500, 800, 1000],
    #     'max_depth': [3, 5, 8, 10, 12],
    #     'min_samples_split': [2, 4, 6, 8, 10],
    #     'min_samples_leaf': [1, 2, 3, 4, 5],
    #     'bootstrap': [True, False,True, False,True]
    # }
    params = {
        # 'alpha': np.linspace(0.1, 10, num=100),
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
    #建立双模型进行数据预测
    rf = RandomForestRegressor(random_state=42)
    gbdt = GradientBoostingRegressor(random_state=42)
    # 建立GridSearchCV对象
    grid_search_rf = GridSearchCV(rf, param_grid=params, cv=5, n_jobs=-1)
    grid_search_gbdt=GridSearchCV(gbdt,param_grid=param_grid,cv=5,n_jobs=-1)
    # 模型训练
    grid_search_rf.fit(X_train, y_train)
    grid_search_gbdt.fit(X_train,y_train)
    # 输出最佳参数
    print("Best parameters_rf:", grid_search_rf.best_params_)
    print("Best parameters_gbdt:", grid_search_gbdt.best_params_)
    # 输出最佳模型评分
    best_model_rf = grid_search_rf.best_estimator_
    best_model_gbdt=grid_search_gbdt.best_estimator_
    y_pred_rf = best_model_rf.predict(X_test)
    y_pred_gbdt=best_model_gbdt.predict(X_test)


    # # 建立模型
    # rf = RandomForestRegressor(random_state=42)
    # # 建立GridSearchCV对象
    # grid_search = GridSearchCV(rf, param_grid=params, cv=5, n_jobs=-1)
    # # 模型训练
    # grid_search.fit(X_train, y_train)
    # # 输出最佳参数
    # print("Best parameters:", grid_search.best_params_)
    # # 输出最佳模型评分
    # best_model = grid_search.best_estimator_
    # y_pred = best_model.predict(X_test)
    # print(y_test)
    # print(np.around(y_pred,3))
    # dic1[f'{i}_预测']=np.around(y_pred,3)
    # print(f"Best model R2 score: {r2_score(y_test, y_pred):.4f}")
    # print(f"Best model RMSE score: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
    print('------------------')
    # 建立模型
    gbdt = GradientBoostingRegressor(random_state=42)
    # 建立GridSearchCV对象
    grid_search = GridSearchCV(gbdt, param_grid=param_grid, cv=5, n_jobs=-1)
    # 模型训练
    grid_search.fit(X_train, y_train)
    # 输出最佳参数
    print("Best parameters:", grid_search.best_params_)
    # 输出最佳模型评分
    best_model = grid_search.best_estimator_
    y_pred = best_model.predict(X_test)
    print(y_test)
    print(np.around(y_pred,3))
    print(f"Best model R2 score: {r2_score(y_test, y_pred):.4f}")
    print(f"Best model RMSE score: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
# df1=pd.DataFrame(dic1)
# print(df1)