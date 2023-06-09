import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from sklearn.datasets import load_boston
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

warnings.filterwarnings('ignore')
# 加载数据集
boston = load_boston()
data = pd.DataFrame(boston.data, columns=boston.feature_names)
data['MEDV'] = boston.target

# 查看数据集基本信息
print(data.info())

# 查看数据分布情况
print(data.describe())

# 查看特征相关性
corr = data.corr()
# sns.heatmap(corr, annot=True, cmap='YlGnBu')

# 特征工程
X = data.drop(['MEDV'], axis=1)
y = data['MEDV']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 特征标准化
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# 建立线性回归模型并训练
lin_reg = LinearRegression()
lin_reg.fit(X_train, y_train)

# 建立随机森林回归模型并训练
rf_reg = RandomForestRegressor(random_state=42)
param_grid = {
    'n_estimators': [100, 300, 500],
    'max_features': ['sqrt', 'log2'],
    'max_depth': [10, 20, 30],
    'min_samples_split': [2, 5, 10],
    'min_samples_leaf': [1, 2, 4]
}
rf_reg_cv = GridSearchCV(rf_reg, param_grid, cv=5)
rf_reg_cv.fit(X_train, y_train)
rf_reg_best = rf_reg_cv.best_estimator_

# 模型预测
lin_reg_pred = lin_reg.predict(X_test)
rf_reg_pred = rf_reg_best.predict(X_test)

# 模型评估
lin_reg_rmse = np.sqrt(mean_squared_error(y_test, lin_reg_pred))
rf_reg_rmse = np.sqrt(mean_squared_error(y_test, rf_reg_pred))
lin_reg_r2 = r2_score(y_test, lin_reg_pred)
rf_reg_r2 = r2_score(y_test, rf_reg_pred)

print(f"线性回归模型的RMSE为：{lin_reg_rmse:.2f}，R2值为：{lin_reg_r2:.2f}")
print(f"随机森林回归模型的RMSE为：{rf_reg_rmse:.2f}，R2值为：{rf_reg_r2:.2f}")
print(f"随机森林回归模型的最佳参数为：{rf_reg_cv.best_params_}")
