# 导入需要的库
import pandas as pd
import numpy as np
import warnings
import matplotlib.pyplot as plt
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import r2_score, mean_squared_error

# 忽略警告
warnings.filterwarnings('ignore')
# 加载数据集
boston = load_boston()

# 转化为DataFrame格式
boston_df = pd.DataFrame(boston.data, columns=boston.feature_names)
boston_df['MEDV'] = boston.target

# 特征工程
boston_df['CRIM_log'] = np.log(boston_df['CRIM'])
boston_df['RM_squared'] = boston_df['RM'] ** 2
boston_df['TAX_log'] = np.log(boston_df['TAX'])
boston_df['LSTAT_log'] = np.log(boston_df['LSTAT'])

# 划分训练集和测试集
X = boston_df.drop('MEDV', axis=1)
y = boston_df['MEDV']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 建立模型列表
models = [
    LinearRegression(),
    Ridge(),
    Lasso(),
    ElasticNet(),
    RandomForestRegressor(),
    GradientBoostingRegressor()
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
    print(f"R2 score: {r2_scores[i]:.4f}")
    print(f"RMSE score: {rmse_scores[i]:.4f}\n")

# 参数调优
params = {
    # 'alpha': np.linspace(0.1, 10, num=100),
    'max_depth': [3, 5, 7],
    'n_estimators': [50, 100, 150]
}

# 建立模型
rf = RandomForestRegressor(random_state=42)

# 建立GridSearchCV对象
grid_search = GridSearchCV(rf, param_grid=params, cv=5, n_jobs=-1)

# 模型训练
grid_search.fit(X_train, y_train)

# 输出最佳参数
print("Best parameters:", grid_search.best_params_)

# 输出最佳模型评分
best_model = grid_search.best_estimator_
y_pred = best_model.predict(X_test)
print(f"Best model R2 score: {r2_score(y_test, y_pred):.4f}")
print(f"Best model RMSE score: {np.sqrt(mean_squared_error(y_test, y_pred)):.4f}")
