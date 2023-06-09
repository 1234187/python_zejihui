import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, confusion_matrix
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import LabelEncoder, MinMaxScaler
from sklearn.tree import DecisionTreeClassifier

# 读取数据
data = pd.read_csv('data.csv')

# 删除id列
data.drop('id', axis=1, inplace=True)

# 将gender列的值编码为0和1
le = LabelEncoder()
data['gender'] = le.fit_transform(data['gender'])

# 对年龄段进行one-hot编码
age_range_onehot = pd.get_dummies(data['age_range'], prefix='age_range')
data = pd.concat([data, age_range_onehot], axis=1)
data.drop('age_range', axis=1, inplace=True)

# 特征工程：生成新的特征
data['credit_score_age'] = data['credit_score'] * data['age_range_1']
data['credit_score_gender'] = data['credit_score'] * data['gender']
data['credit_age_gender'] = data['credit_score'] * data['age_range_1'] * data['gender']
data['balance_age'] = data['balance'] * data['age_range_1']
data['balance_gender'] = data['balance'] * data['gender']
data['balance_age_gender'] = data['balance'] * data['age_range_1'] * data['gender']

# 数据预处理：归一化
scaler = MinMaxScaler()
data[['credit_score', 'balance']] = scaler.fit_transform(data[['credit_score', 'balance']])

# 划分训练集和测试集
X = data.drop('is_fraud', axis=1)
y = data['is_fraud']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 决策树模型
dtc = DecisionTreeClassifier(random_state=42)
dtc_params = {'criterion': ['gini', 'entropy'], 'max_depth': [5, 10, 15]}
dtc_gs = GridSearchCV(dtc, dtc_params, cv=5, scoring='f1')
dtc_gs.fit(X_train, y_train)
dtc_best = dtc_gs.best_estimator_

# 随机森林模型
rfc = RandomForestClassifier(random_state=42)
rfc_params = {'n_estimators': [100, 200, 300], 'max_depth': [5, 10, 15]}
rfc_gs = GridSearchCV(rfc, rfc_params, cv=5, scoring='f1')
rfc_gs.fit(X_train, y_train)
rfc_best = rfc_gs.best_estimator_

# 评估模型
def evaluate_model(model, X_test, y_test):
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred)
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    specificity = tn / (tn + fp)
    print('Accuracy:', acc)
    print('F1-score:', f1)
    print('AUC:', auc)
    print('Specificity:', specificity)

print('Decision Tree Model Evaluation:')
evaluate_model
