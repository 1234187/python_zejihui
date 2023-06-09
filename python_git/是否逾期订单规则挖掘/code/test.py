import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.tree import _tree

# 数据生成
data = pd.DataFrame({
    'gender': np.random.choice(['0', '1'], size=5000),
    'age': np.random.randint(18, 60, size=5000),
    'province': np.random.choice(['0', '1', '2'], size=5000),
    'city': np.random.choice(['0', '1', '2'], size=5000),
    'rent_range': np.random.randint(30,2000, size=5000),
    'brand': np.random.choice(['0', '1', '2'], size=5000),
    'memory': np.random.choice(['0', '1', '2'], size=5000),
    'is_screen_insured': np.random.choice([0, 1], size=5000), 'is_overdue': np.random.choice([0, 1], size=5000)
})

# 选择特征和目标变量
X = data[['gender', 'age', 'province', 'city', 'rent_range', 'brand', 'memory', 'is_screen_insured']]
y = data['is_overdue']

# 数据集分割
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)

# 构建决策树
tree = DecisionTreeClassifier(max_depth=5, min_samples_leaf=50)
tree.fit(X_train, y_train)


# 定义规则提取函数
def tree_to_rules(tree, feature_names):
    rules = []
    tree_ = tree.tree_
    feature_name = [
        feature_names[i] if i != _tree.TREE_UNDEFINED else "undefined!"
        for i in tree_.feature
    ]

    def recurse(node, rule):
        if tree_.feature[node] != _tree.TREE_UNDEFINED:
            name = feature_name[node]
            threshold = tree_.threshold[node]
            rules.append(rule + "if {} <= {}".format(name, threshold))
            recurse(tree_.children_left[node], rule + "   ")
            rules.append(rule + "if {} > {}".format(name, threshold))
            recurse(tree_.children_right[node], rule + "   ")
        else:
            rules.append(rule + "return {}".format(tree_.value[node]))

    recurse(0, "")
    return rules


# 导出并显示规则
rules = tree_to_rules(tree, X.columns)
print(rules)

# 模型评估
accuracy = tree.score(X_test, y_test)
print('模型准确度为:', accuracy)