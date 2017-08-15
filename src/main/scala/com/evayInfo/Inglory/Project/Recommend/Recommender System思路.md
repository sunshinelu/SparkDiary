# Recommender System思路

构建组合模型：

1. ALS Model

ALS Model的结果保存在`Recommender_als` 表中。


2. Content-Based Model

Content-Based Model的结果保存在`Recommender_CB` 表中。

3. User-based Model

User-Based Model的结果保存在`Recommender_UB` 表中。

4. Item-based Model

Item-Based Model的结果保存在`Recommender_IB` 表中。


根据不同的业务需求对子模型赋予不同的权重，

`rowkey`：
`info: userID`：用户名
`info: id`：文章I（与yilan-total_webpage表中的roekey相对应）
`info: rating`：
`info: rn`：排序
`info: title`：文章标题
`info: manuallabel`：文章的标签
`info: mod`：文章的时间
`info: sysTime`：系统时间
