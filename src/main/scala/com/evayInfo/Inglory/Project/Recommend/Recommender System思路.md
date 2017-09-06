# Recommender System思路

## 1、组合模型的构建：

组合模型组成：

ALS Model、Content-Based Model、User-based Model和Item-based Model
（暂定）

根据不同的业务需求对子模型赋予不同的权重，例如，当没有明显业务倾向时，这4个模型的权重均为0.25.

### 1. ALS Model

思路：使用ALS算法构建ALS推荐模型。

结果：ALS Model的结果保存在`recommender_als` 表中。

任务提交代码：

spark-submit --class com.evayInfo.Inglory.Project.Recommend.alsModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
yilan-total_webpage t_hbaseSink  recommender_als

count 'recommender_als'

### 2. Content-Based Model

思路：计算文章的相似性，得出文章相似性矩阵，然后根据用户的历史浏览记录想用户推荐未浏览过，但相似性打分较高的文章。

结果：Content-Based Model的结果保存在`recommender_content` 表中。

spark-submit --class com.evayInfo.Inglory.Project.Recommend.contentModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
yilan-total_webpage t_hbaseSink docsimi_word2vec recommender_content

### 3. User-based Model

思路：使用用户日志数据，计算用户之间的相似，想用户推荐与其相似用户浏览过且自身未浏览过的文章。

结果：User-Based Model的结果保存在`recommender_user` 表中。

任务提交代码：

spark-submit --class com.evayInfo.Inglory.Project.Recommend.userModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
yilan-total_webpage t_hbaseSink  recommender_user


count 'recommender_user'


### 4. Item-based Model

思路：使用用户日志数据，计算文章之间的相似，然后根据用户的历史浏览记录想用户推荐未浏览过，但相似性打分较高的文章。

结果：Item-Based Model的结果保存在`recommender_item` 表中。


任务提交代码：

spark-submit --class com.evayInfo.Inglory.Project.Recommend.itemModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
yilan-total_webpage t_hbaseSink  recommender_item

count 'recommender_item'

**注意**：Content-Based Model和Item-based Model均生成item-item similarity矩阵，后期过滤流程一致。


### 5. Combined Model

思路: 通过整合ALS Model、Content-Based Model、User-based Model和Item-based Model模型构建Combined Model。

结果：Combined Model的结果保存在`recommender_combined` 表中。

任务提交代码：

spark-submit --class com.evayInfo.Inglory.Project.Recommend.combineModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
recommender_als recommender_content  recommender_user recommender_item recommender_combined


修改分析方案，直接获取子模型分析结果，不把子模型结果保存到hbase中。

spark-submit --class com.evayInfo.Inglory.Project.Recommend.combinedModel \
--master yarn \
--num-executors 2 \
--executor-cores 2 \
--executor-memory 4g \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/recommend/SparkDiary.jar \
yilan-total_webpage t_hbaseSink  ylzx_xgwz ylzx_cnxh_combined

（在combinedModel中添加` println("df1数量为：" + df1.count())`任务才运行成功，
应该是spark data frame中的优化逻辑导致。目前尚未找到解决方案。）

## 2、HBase表的设计



`rowkey`：rowkey命名规则为`userID` + score + `rn`

`info: userID`：用户名

`info: id`：文章I（与yilan-total_webpage表中的roekey相对应）

`info: rating`：文章打分

`info: rn`：排序

`info: title`：文章标题

`info: manuallabel`：文章的标签

`info: mod`：文章的时间

`info: sysTime`：系统时间


## 3、代码的设计

1. 在代码中新建HBase表，先判断是否有该表，如果有的话则不做任何操作，如果没有的话则新建。

参考代码如下：

        val inputTable = "输入表名"
        val outputTable = "输出表名"

        val conf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
        //设置查询的表名
        conf.set(TableInputFormat.INPUT_TABLE, inputTable) //设置输入表名
       //指定输出格式和输出表名
        conf.set(TableOutputFormat.OUTPUT_TABLE, outputTable) //设置输出表名

        //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
        val hadmin = new HBaseAdmin(conf)
        if (!hadmin.isTableAvailable(outputTable)) {
          print("Table Not Exists! Create Table")
          val tableDesc = new HTableDescriptor(TableName.valueOf(outputTable))
          tableDesc.addFamily(new HColumnDescriptor("p".getBytes()))
          tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
          hadmin.createTable(tableDesc)
        }else{
          print("Table  Exists!  not Create Table")
        }

2.

