# docsSimilarity分析思路

计算doc-doc similarity的方法：

方法一：使用word2vec方法生成features => docsimi_word2vec

方法二：使用CountVectorizer方法限制词的个数，生成features => docsimi_count

方法三：使用item-based推荐算法计算item-item similarity => docsimi_item

方法四：使用ALS推荐算法计算item-item similarity(rank值为features的长度) => docsimi_als

=> docsimi

## 1. 构建词典

构建Word2Vec模型，根据热词寻找相关词，并构建词典。

构建词典方法：

* 提取文章关键词，使用关键词构建词典

* 构建LDA主题模型，使用主题词构建词典

* 人工干预


## 2. 构建Word2Vec模型

使用上一步生成的词典，对分词结果进行过滤，使数据中仅包含词典中的词。

## 3. 计算文章相似性

* 使用Word2Vec模型生成的features计算文章的相似性

* 使用item-based方法计算item-item similarity



提交任务代码：（参考）

docsSimi_word2vec.sh

    #!/bin/bash
    #name=$1
    
    set -x
    source /root/.bashrc
    cd /root/software/spark-2.0.2/bin

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarity \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage docsimi_word2vec


## 4. 结果输出形式

表名：

docsSimi_word2vec

表结构：

rowkey
info: simsScore => 相似性打分
info: level => 排名
info: simsID => 相似文章ID
info: t => 相似文章标题
info: manuallabel => 相似文章标签
info: mod => 相似文章时间
info: websitename => 相似文章网站名
info: id => urlID


## 5. 任务运行日志

### 时间：2017年8月17日

#### 执行任务代码：

服务器：slave6

spark版本：2.1.0

    spark-submit --class com.evayInfo.Inglory.Project.DocsSimilarity.bulidWord2VecModel \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage

任务报错：

    Exception in thread "main" java.lang.OutOfMemoryError: Java heap space
        at org.apache.spark.mllib.feature.Word2Vec.fit(Word2Vec.scala:340)
        at org.apache.spark.ml.feature.Word2Vec.fit(Word2Vec.scala:185)
        at com.evayInfo.Inglory.Project.DocsSimilarity.bulidWord2VecModel$.main(bulidWord2VecModel.scala:110)
        at com.evayInfo.Inglory.Project.DocsSimilarity.bulidWord2VecModel.main(bulidWord2VecModel.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:606)
        at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:738)
        at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:187)
        at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:212)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:126)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
	

解决方案：

将setVectorSize参数改为10（设置为50时，程序运行时间大于3小时）

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("segWords")
      .setOutputCol("features")
      .setVectorSize(50) // 1000
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(segDF)

重新提交任务

任务运行时间：16mins, 18sec

查看结果：

    hadoop fs -ls /personal/sunlu/Project/docsSimi/Word2VecModelDF_dic

#### 执行任务docsSimilarity：计算文章相似性

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarity \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 6g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage docsimi_word2vec



任务报错：

1.

	ExecutorLostFailure (executor 4 exited caused by one of the running tasks) Reason:
	 Container killed by YARN for exceeding memory limits. 4.5 GB of 4.5 GB physical memory used.
	 Consider boosting spark.yarn.executor.memoryOverhead.

2.

	 	TaskKilled (killed intentionally)

3. 

    org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 2
   	
#### 运行任务docsSimiTest
   	
   	
    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimiTest \
    --master yarn \
    --num-executors 8 \
    --executor-cores 8 \
    --executor-memory 6g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    t_ylzx_sun t_docsimi_word2vec
    
    spark-submit --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimiTest --master yarn --num-executors 4 --executor-cores 4 --executor-memory 6g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar /root/lulu/Progect/docsSimi/SparkDiary.jar t_ylzx_sun t_docsimi
    
    
    spark-submit --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimiTest --master yarn --num-executors 4 --executor-cores 4 --executor-memory 6g --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar /root/lulu/Progect/docsSimi/SparkDiary.jar t_ylzx_sun t_docsimi
    
### 时间：2017年8月21日

#### 执行`bulidWord2VecModel`任务代码：

服务器：slave6

spark版本：2.1.0

        // Learn a mapping from words to Vectors.
        val word2Vec = new Word2Vec()
          .setInputCol("segWords")
          .setOutputCol("features")
          .setVectorSize(1) // 1000
          .setMinCount(1)
        val word2VecModel = word2Vec.fit(segDF)
        word2VecModel.write.overwrite().save("/personal/sunlu/Project/docsSimi/Word2VecModelDF")

运行任务：

    spark-submit --class com.evayInfo.Inglory.Project.DocsSimilarity.bulidWord2VecModel \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage

查看运行结果

    hadoop fs -ls /personal/sunlu/Project/docsSimi/Word2VecModelDF
    
#### 执行任务docsSimilarity：计算文章相似性

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarity \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage docsimi_word2vec
    
报错：

    Exception in thread "main" java.lang.UnsupportedOperationException: empty collection
        at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$apply$36.apply(RDD.scala:1027)
        at org.apache.spark.rdd.RDD$$anonfun$reduce$1$$anonfun$apply$36.apply(RDD.scala:1027)
        at scala.Option.getOrElse(Option.scala:121)
        at org.apache.spark.rdd.RDD$$anonfun$reduce$1.apply(RDD.scala:1027)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
        at org.apache.spark.rdd.RDD.withScope(RDD.scala:362)
        at org.apache.spark.rdd.RDD.reduce(RDD.scala:1007)
        at org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix.numRows(IndexedRowMatrix.scala:66)
        at org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix.toCoordinateMatrix(IndexedRowMatrix.scala:130)
        at com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarity$.main(docsSimilarity.scala:183)
        at com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarity.main(docsSimilarity.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:606)
        at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:738)
        at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:187)
        at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:212)
        at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:126)
        at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
        
        
运行代码：

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.docsSimilarityV2 \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage docsimi_word2vec


spark-shell --master yarn --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar 