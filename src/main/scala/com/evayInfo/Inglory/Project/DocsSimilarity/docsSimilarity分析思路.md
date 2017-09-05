# docsSimilarity分析思路

计算doc-doc similarity的方法：

方法一：使用word2vec方法生成features => docsimi_word2vec (运行时间 > 3小时)

方法二：使用CountVectorizer方法限制词的个数，生成features => docsimi_count (运行时间 > 3小时)

方法三：使用item-based推荐算法计算item-item similarity => docsimi_item (结果受用户浏览日志限制)

方法四：使用ALS推荐算法计算item-item similarity(rank值为features的长度) => docsimi_als (结果受用户浏览日志限制)

方法五：使用LDA算法生成features计算文章相似性 => docsimi_lda ()

方法六：使用SVD算法计算文章相似性 => docsimi_svd (运行时间 > 3小时)

方法七：使用Jaccard算法（MinHashLSH）计算文章相似性 => docsimi_jaccard (运行时间在1小时以内，但是数据缺失严重。)

方法八：使用Euclidean算法计算文章相似性 => docsimi_euclidean (运行时间 > 3小时)

方法九：使用文章标题计算文章相似性 => docsimi_title ()
       使用“标题＋内容关键词＋manuallabel”计算文章相似性()


word2vec                       

CountVectorizer                      cosine

item-based         

ALS                                  Jaccard  

LDA

SVD                                  Euclidean

标题＋内容关键词＋manuallabel

=> docsimi

表结构：

`rowkey`
`info`:`id`
`info`:`simsID`
`info`:`simsScore`
`info`:`level`
`info`:`t`
`info`:`manuallabel`
`info`:`websitename`
`info`:`mod`



方法四：使用ALS推荐算法计算item-item similarity(rank值为features的长度) => docsimi_als

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiALS \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage t_hbaseSink docsimi_als

方法六：使用SVD算法计算文章相似性 => docsimi_svd

    spark-submit \
    --class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiSVD \
    --master yarn \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 4g \
    --jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
    /root/lulu/Progect/docsSimi/SparkDiary.jar \
    yilan-total_webpage docsimi_svd

方法七：使用Jaccard算法（MinHashLSH）计算文章相似性 => docsimi_Jaccard

spark-submit \
--class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiJaccard \
--master yarn \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=100 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/docsSimi/SparkDiary.jar \
yilan-total_webpage docsimi_jaccard


ylzxRDD.count
res0: Long = 105955

count 'docsimi_jaccard'
=> 264318

count 'docsimi_jaccard'
=> 252262

========================
 val mh = new MinHashLSH().
      setNumHashTables(10).//改为10
      setInputCol("tfidfVec").
      setOutputCol("mhVec")

           ｜｜
           ｜｜
           \  /
            \/
1hrs, 7mins, 51sec

count 'docsimi_jaccard'
=> 304427

=========================over

=========================

 val mh = new MinHashLSH().
      setNumHashTables(30).//改为30
      setInputCol("tfidfVec").
      setOutputCol("mhVec")

           ｜｜
           ｜｜
           \  /
            \/

org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle 2

=========================over

=========================

 val mh = new MinHashLSH().
      setNumHashTables(20).//改为20
      setInputCol("tfidfVec").
      setOutputCol("mhVec")

           ｜｜
           ｜｜
           \  /
            \/

在slave6下使用yarn－cluster模式提交任务


spark-submit \
--class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiJaccard \
--master yarn \
--deploy-mode cluster \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
/root/lulu/Progect/docsSimi/SparkDiary-1.0-SNAPSHOT-jar-with-dependencies.jar \
yilan-total_webpage docsimi_jaccard


任务运行时间：1hrs, 24mins, 12sec

count 'docsimi_jaccard'

=> 325139

=========================over

=========================

val vocabSize: Int = 200000

           ｜｜
           ｜｜
           \  /
            \/

在slave6下使用yarn－cluster模式提交任务


2hrs, 33mins, 18sec

count 'docsimi_jaccard'

=> 377740

=========================over


=========================

spark-submit \
--class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiJaccard \
--master yarn \
--deploy-mode cluster \
--num-executors 12 \
--executor-cores 3 \
--executor-memory 3g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.4 \
--conf spark.shuffle.memoryFraction=0.6 \
/root/lulu/Progect/docsSimi/SparkDiary-1.0-SNAPSHOT-jar-with-dependencies.jar \
yilan-total_webpage docsimi_jaccard

任务被全部清空，不知是否是由于executors的数量设置过大导致。


=========================over

=========================

    val hashingTF = new HashingTF().
      setInputCol("segWords").setOutputCol("tfFeatures")//.setNumFeatures(20)
    val tfData = hashingTF.transform(ylzxDS)

    val idf = new IDF().setInputCol("tfFeatures").setOutputCol("tfidfVec")
    val idfModel = idf.fit(tfData)
    val tfidfDF = idfModel.transform(tfData)

count 'docsimi_jaccard'
=> 415388


=========================over


get 'docsimi_jaccard','fef4e4b8-6a4a-4b5d-8356-72050e3480d9::score=1'

    hbase(main):002:0> get 'docsimi_jaccard','fef4e4b8-6a4a-4b5d-8356-72050e3480d9::score=1'
    COLUMN                    CELL
     info:id                  timestamp=1504086034736, value=fef4e4b8-6a4a-4b5d-8356-72050e3480d9
     info:level               timestamp=1504086034736, value=1
     info:manuallabel         timestamp=1504086034736, value=\xE4\xBA\xBA\xE6\x89\x8D
     info:mod                 timestamp=1504086034736, value=2017-08-21
     info:simsID              timestamp=1504086034736, value=62980453-ef59-4e43-946d-5c1672b745b1
     info:simsScore           timestamp=1504086034736, value=0.8834745762711864
     info:t                   timestamp=1504086034736, value=\xE6\xBB\xA8\xE5\xB7\x9E\xE6\x83\xA0\xE
                              6\xB0\x91\xE5\x8E\xBF\xE6\x9D\x8E\xE5\xBA\x84\xE9\x95\x87\xE6\x89\x93\
                              xE9\x80\xA0\xE5\x85\xA8\xE5\x9B\xBD\xE7\xBB\xB3\xE7\xBD\x91\xE7\x89\xB
                              9\xE8\x89\xB2\xE5\xB0\x8F\xE9\x95\x87
     info:websitename         timestamp=1504086034736, value=\xE5\x87\xA4\xE5\x87\xB0\xE7\xBD\x91
    8 row(s) in 0.0960 seconds



get 'docsimi_jaccard','fef4e4b8-6a4a-4b5d-8356-72050e3480d9::score=2'

    hbase(main):003:0> get 'docsimi_jaccard','fef4e4b8-6a4a-4b5d-8356-72050e3480d9::score=2'
    COLUMN                    CELL
     info:id                  timestamp=1504086034736, value=fef4e4b8-6a4a-4b5d-8356-72050e3480d9
     info:level               timestamp=1504086034736, value=2
     info:manuallabel         timestamp=1504086034736, value=\xE6\x94\xBF\xE5\x8A\xA1
     info:mod                 timestamp=1504086034736, value=2017-05-22
     info:simsID              timestamp=1504086034736, value=cn.gov.tjec.www:http/newzixun/61011.htm
     info:simsScore           timestamp=1504086034736, value=0.8858131487889274
     info:t                   timestamp=1504086034736, value=\xE4\xBF\x9D\xE7\xA8\x8E\xE5\x8C\xBA\xE
                              5\x8A\xA0\xE9\x80\x9F\xE5\xB8\x83\xE5\xB1\x80\xE4\xBA\xA7\xE5\x9F\x8E\
                              xE8\x9E\x8D\xE5\x90\x88
     info:websitename         timestamp=1504086034736, value=\xE5\xA4\xA9\xE6\xB4\xA5\xE5\xB8\x82\xE
                              5\xB7\xA5\xE4\xB8\x9A\xE5\x92\x8C\xE4\xBF\xA1\xE6\x81\xAF\xE5\x8C\x96\
                              xE5\xA7\x94\xE5\x91\x98\xE4\xBC\x9A


http://www.sdny.gov.cn/snzx/dfdt/201708/t20170804_676565.html

simi:

http://news.ifeng.com/a/20170821/51702644_0.shtml
http://www.tjec.gov.cn/newzixun/61011.htm




方法八：使用Euclidean算法计算文章相似性 => docsimi_euclidean

spark-submit \
--class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiEuclidean \
--master yarn \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/docsSimi/SparkDiary.jar \
yilan-total_webpage docsimi_euclidean


方法九：使用文章标题计算文章相似性 => docsimi_title

spark-submit \
--class com.evayInfo.Inglory.Project.DocsSimilarity.DocsimiTitle \
--master yarn \
--num-executors 8 \
--executor-cores 4 \
--executor-memory 6g \
--conf spark.default.parallelism=150 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.shuffle.memoryFraction=0.4 \
--jars /root/software/extraClass/ansj_seg-3.7.6-all-in-one.jar \
/root/lulu/Progect/docsSimi/SparkDiary.jar \
yilan-total_webpage docsimi_title




export JAVA_OPTIONS=-XX:-UseGCOverheadLimit





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