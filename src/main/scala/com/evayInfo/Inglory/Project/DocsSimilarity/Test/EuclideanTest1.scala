package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/30.
 * distCol值越小越相似，distCol为0时完全相同，distCol值越大越不相同
 */
object EuclideanTest1 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"EuclideanTest1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val documents = sc.textFile("file:///Users/sunlu/Documents/workspace/IDEA/Github/SparkDiary/data/documents.txt").map(_.split(" ").toSeq).
      zipWithUniqueId().toDF("segWords", "id")

    val vocabSize: Int = 20000

    val vocabModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("segWords").
      setOutputCol("features").
      setVocabSize(vocabSize).
      setMinDF(1).
      fit(documents)
    val docTermFreqs = vocabModel.transform(documents)
    val idf = new IDF().setInputCol("features").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val tfidfDF = idfModel.transform(docTermFreqs) //.select("id", "tfidfVec")
    //    tfidfDF.printSchema()
    /*
    root
     |-- segWords: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- id: long (nullable = true)
     |-- features: vector (nullable = true)
     |-- tfidfVec: vector (nullable = true)
     */

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("tfidfVec")
      .setOutputCol("brpVec")

    val brpModel = brp.fit(tfidfDF)

    // Feature Transformation
    val brpTransformed = brpModel.transform(tfidfDF)
    //    println("brpTransformed is: ")
    //    brpTransformed.printSchema()
    /*
    root
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- id: long (nullable = true)
 |-- features: vector (nullable = true)
 |-- tfidfVec: vector (nullable = true)
 |-- brpVec: array (nullable = true)
 |    |-- element: vector (containsNull = true)
     */
    //    brpTransformed.take(100).foreach(println)
    /*
[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])]
[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])]
[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-1.0], [-1.0], [0.0])]
[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])]
[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])]
[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [0.0], [-1.0])]

     */

    val df1 = brpModel.approxSimilarityJoin(brpTransformed, brpTransformed, 2.5)
    println("df1 is: ")
    println("df1 count is: " + df1.count())
    /*
    2.5 => df1 count is: 22
    5.0 => df1 count is: 30
    8.0 => df1 count is: 30
    10.0 => df1 count is: 34
    50.0 => df1 count is: 34
    100.0 => df1 count is: 34
    500.0 => df1 count is: 34
    1000.0 => df1 count is: 30
     */
    //    df1.printSchema()
    /*
    root
 |-- datasetA: struct (nullable = false)
 |    |-- segWords: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- id: long (nullable = true)
 |    |-- features: vector (nullable = true)
 |    |-- tfidfVec: vector (nullable = true)
 |    |-- brpVec: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |-- datasetB: struct (nullable = false)
 |    |-- segWords: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- id: long (nullable = true)
 |    |-- features: vector (nullable = true)
 |    |-- brpVec: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |    |-- tfidfVec: vector (nullable = true)
 |-- distCol: double (nullable = true)
     */
    //    df1.take(100).foreach(println)
    /*
    [[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([0.0], [0.0], [0.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],2.474610743804057]
    [[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),WrappedArray([0.0], [-1.0], [0.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])],2.4192028079597168]
    [[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),WrappedArray([0.0], [-1.0], [0.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])],2.487984499678467]
    [[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368])],0.0]
    [[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([0.0], [0.0], [0.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],0.0]
    [[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],2.487984499678467]
    [[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),WrappedArray([0.0], [-1.0], [0.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])],1.9638594080746683]
    [[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([0.0], [0.0], [0.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],2.487984499678467]
    [[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([0.0], [0.0], [0.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],2.33926338970702]
    [[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368])],2.487984499678467]
    [[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])],[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[12,13],[1.252762968495368,1.252762968495368])],2.474610743804057]
    [[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],0.8472978603872037]
    [[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],2.33926338970702]
    [[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368])],2.33926338970702]
    [[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),WrappedArray([0.0], [-1.0], [0.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])],0.0]
    [[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368])],1.9638594080746683]
    [[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),WrappedArray([0.0], [0.0], [0.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],0.8472978603872037]
    [[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[12,13],[1.252762968495368,1.252762968495368])],0.0]
    [[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.0], [0.0], [-1.0])],[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [0.0], [-1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368])],0.0]
    [[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([0.0], [0.0], [0.0])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),WrappedArray([0.0], [-1.0], [0.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])],2.33926338970702]
    [[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([0.0], [-1.0], [0.0])],[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[12,13],[1.252762968495368,1.252762968495368])],2.4192028079597168]
    [[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-1.0], [-1.0], [0.0])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),WrappedArray([-1.0], [-1.0], [0.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])],0.0]

     */

    //    df1.select("datasetA.id", "datasetA.segWords", "datasetB.id", "datasetB.segWords", "distCol").take(100).foreach(println)
    /*
    [1,WrappedArray(hello, word),0,WrappedArray(today, is, a, good, day),2.474610743804057]
    [1,WrappedArray(hello, word),2,WrappedArray(I, am, a, girl),2.4192028079597168]
    [4,WrappedArray(today, is, not, a, good, day),2,WrappedArray(I, am, a, girl),2.487984499678467]
    [3,WrappedArray(I, am, not, a, boy),3,WrappedArray(I, am, not, a, boy),0.0]
    [0,WrappedArray(today, is, a, good, day),0,WrappedArray(today, is, a, good, day),0.0]
    [2,WrappedArray(I, am, a, girl),4,WrappedArray(today, is, not, a, good, day),2.487984499678467]
    [3,WrappedArray(I, am, not, a, boy),2,WrappedArray(I, am, a, girl),1.9638594080746683]
    [3,WrappedArray(I, am, not, a, boy),0,WrappedArray(today, is, a, good, day),2.487984499678467]
    [2,WrappedArray(I, am, a, girl),0,WrappedArray(today, is, a, good, day),2.33926338970702]
    [0,WrappedArray(today, is, a, good, day),3,WrappedArray(I, am, not, a, boy),2.487984499678467]
    [0,WrappedArray(today, is, a, good, day),1,WrappedArray(hello, word),2.474610743804057]
    [0,WrappedArray(today, is, a, good, day),4,WrappedArray(today, is, not, a, good, day),0.8472978603872037]
    [3,WrappedArray(I, am, not, a, boy),4,WrappedArray(today, is, not, a, good, day),2.33926338970702]
    [4,WrappedArray(today, is, not, a, good, day),3,WrappedArray(I, am, not, a, boy),2.33926338970702]
    [2,WrappedArray(I, am, a, girl),2,WrappedArray(I, am, a, girl),0.0]
    [2,WrappedArray(I, am, a, girl),3,WrappedArray(I, am, not, a, boy),1.9638594080746683]
    [4,WrappedArray(today, is, not, a, good, day),0,WrappedArray(today, is, a, good, day),0.8472978603872037]
    [1,WrappedArray(hello, word),1,WrappedArray(hello, word),0.0]
    [5,WrappedArray(mom, I, love, you),5,WrappedArray(mom, I, love, you),0.0]
    [0,WrappedArray(today, is, a, good, day),2,WrappedArray(I, am, a, girl),2.33926338970702]
    [2,WrappedArray(I, am, a, girl),1,WrappedArray(hello, word),2.4192028079597168]
    [4,WrappedArray(today, is, not, a, good, day),4,WrappedArray(today, is, not, a, good, day),0.0]

     */


    sc.stop()
    spark.stop()
  }

}
