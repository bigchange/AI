# AI

## 电影推荐模型 ##

*数据来源*

http://www.grouplens.org/datasets/movielens/

*数据格式*

#### ratings.dat ####

UserID::MovieID::Rating::Timestamp

#### users.dat ####
UserID::Gender::Age::Occupation::Zip-code

#### movies.dat ####
MovieID::Title::Genres

#### personalMoviesRatings.dat ####
UserID::MovieID::Rating::Timestamp

## 构建分类模型 ##

* 常见分类模型*
线性模型、决策树、朴素贝叶斯

线性模型：
1.逻辑回归：概率模型，预测摸个数据点属于正类的概率估计
2.支持向量机：对等连接函数估计值大于或等于阈值0时，标记1 否则标记0（阈值为自适应模型参数）

朴素贝叶斯：概率模型，假设各个特征之间条件独立，属于某个类别的概率->若干个概率之积（特征在给定某个类别下出现的条件概率和该类别的先验概率，这些概率都可以通过数据的频率估计得到）
分类过程：就是在给定特征和类别概率的情况下选择最可能的类别

决策树：非概率模型，表达复杂的非线性模式和特征相互关系，处理类属和数值特征，不需要输入股数据归一或标准化，适用集成方法，形成决策森林
每一个特征的决策通过评估特征分裂的信息增益，最后选择分割数据集最优的特征
评估方法：基尼不纯和熵值



*损失函数*
1、logistic loss -> 逻辑回归模型  2、hinge loss -> SVM模型



