package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, MinHashLSH}
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/8/30.
 * distCol值越小越相似，distCol为0时完全相同，distCol为1时完全不相同
 */
object MinHashLSHTest1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {

    SetLogger

    val sparkConf = new SparkConf().setAppName(s"MinHashLSHTest1").setMaster("local[*]").set("spark.executor.memory", "2g")
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

    val mh = new MinHashLSH()
      .setNumHashTables(3)
      .setInputCol("tfidfVec")
      .setOutputCol("mhVec")

    val mhModel = mh.fit(tfidfDF)
    val mhTransformed = mhModel.transform(tfidfDF)
    //    mhTransformed.printSchema()
    /*
    root
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- id: long (nullable = true)
 |-- features: vector (nullable = true)
 |-- tfidfVec: vector (nullable = true)
 |-- mhVec: array (nullable = true)
 |    |-- element: vector (containsNull = true)
     */
    //    mhTransformed.take(100).foreach(println)
    /*
[WrappedArray(today, is, a, good, day),0,(15,[0,2,4,5,6],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])]
[WrappedArray(I, am, a, girl),2,(15,[0,1,3,8],[1.0,1.0,1.0,1.0]),(15,[0,1,3,8],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.974047307E9])]
[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,4,5,6,7],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,4,5,6,7],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])]
[WrappedArray(hello, word),1,(15,[10,13],[1.0,1.0]),(15,[10,13],[1.252762968495368,1.252762968495368]),WrappedArray([-3.03355771E8], [-2.9787486E7], [-8.93031194E8])]
[WrappedArray(I, am, not, a, boy),3,(15,[0,1,3,7,11],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,3,7,11],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.278435698E9], [-1.974869772E9], [-1.974047307E9])]
[WrappedArray(mom, I, love, you),5,(15,[1,9,12,14],[1.0,1.0,1.0,1.0]),(15,[1,9,12,14],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.636950479E9])]

     */

    val df1 = mhModel.approxSimilarityJoin(mhTransformed, mhTransformed, 0.9)
    println("df1 is: ")
    println(df1.count())
    //    df1.printSchema()
    /*
    root
 |-- datasetA: struct (nullable = false)
 |    |-- segWords: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- id: long (nullable = true)
 |    |-- features: vector (nullable = true)
 |    |-- tfidfVec: vector (nullable = true)
 |    |-- mhVec: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |-- datasetB: struct (nullable = false)
 |    |-- segWords: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- id: long (nullable = true)
 |    |-- features: vector (nullable = true)
 |    |-- tfidfVec: vector (nullable = true)
 |    |-- mhVec: array (nullable = true)
 |    |    |-- element: vector (containsNull = true)
 |-- distCol: double (nullable = true)
     */
    //        df1.filter("datasetA.id < datasetB.id").show()//.take(10000)foreach(println)
    /*
[[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],0.875]
[[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],0.7777777777777778]
[[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],0.0]
[[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],0.8571428571428572]
[[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],0.8888888888888888]
[[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],0.875]
[[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.16666666666666663]
[[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.875]
[[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],0.5]
[[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],0.0]
[[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.16666666666666663]
[[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.8888888888888888]
[[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.0]
[[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],0.5]
[[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],0.0]
[[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],0.875]
[[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.8888888888888888]
[[WrappedArray(I, am, not, a, boy),3,(15,[0,1,6,7,14],[1.0,1.0,1.0,1.0,1.0]),(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]),WrappedArray([-1.586867511E9], [-1.974869772E9], [-1.974047307E9])],[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.7777777777777778]
[[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([1.202372007E9], [4.02453022E8], [-1.636950479E9])],[WrappedArray(hello, word),1,(15,[12,13],[1.0,1.0]),(15,[12,13],[1.252762968495368,1.252762968495368]),WrappedArray([1.202372007E9], [4.02453022E8], [-1.636950479E9])],0.0]
[[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(today, is, a, good, day),0,(15,[0,2,3,4,5],[1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],0.0]
[[WrappedArray(today, is, not, a, good, day),4,(15,[0,2,3,4,5,6],[1.0,1.0,1.0,1.0,1.0,1.0]),(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]),WrappedArray([-2.031299587E9], [-1.758749518E9], [-1.974047307E9])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],0.8888888888888888]
[[WrappedArray(mom, I, love, you),5,(15,[1,8,10,11],[1.0,1.0,1.0,1.0]),(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]),WrappedArray([-1.809083549E9], [-1.974869772E9], [-1.230128022E9])],[WrappedArray(I, am, a, girl),2,(15,[0,1,7,9],[1.0,1.0,1.0,1.0]),(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]),WrappedArray([-1.05621966E9], [-1.974869772E9], [-1.974047307E9])],0.8571428571428572]

     */

    val df2 = df1.select("datasetA.id", "datasetB.id", "distCol")
    //    println("df2 is: ")
    //    df2.printSchema()
    /*
    root
 |-- id: long (nullable = true)
 |-- id: long (nullable = true)
 |-- distCol: double (nullable = true)
     */
    //    df2.take(10000)foreach(println)
    /*
    [0,2,0.875]
    [4,3,0.7777777777777778]
    [5,5,0.0]
    [2,5,0.8571428571428572]
    [0,3,0.8888888888888888]
    [5,3,0.875]
    [4,0,0.16666666666666663]
    [2,0,0.875]
    [2,3,0.5]
    [3,3,0.0]
    [0,4,0.16666666666666663]
    [3,0,0.8888888888888888]
    [4,4,0.0]
    [3,2,0.5]
    [2,2,0.0]
    [3,5,0.875]
    [2,4,0.8888888888888888]
    [3,4,0.7777777777777778]
    [1,1,0.0]
    [0,0,0.0]
    [4,2,0.8888888888888888]
    [5,2,0.8571428571428572]
     */


    //    df1.select("datasetA.id","datasetA.segWords", "datasetB.id","datasetB.segWords","distCol").take(100).foreach(println)
    /*

    [0,WrappedArray(today, is, a, good, day),2,WrappedArray(I, am, a, girl),0.875]
    [4,WrappedArray(today, is, not, a, good, day),3,WrappedArray(I, am, not, a, boy),0.7777777777777778]
    [5,WrappedArray(mom, I, love, you),5,WrappedArray(mom, I, love, you),0.0]
    [2,WrappedArray(I, am, a, girl),5,WrappedArray(mom, I, love, you),0.8571428571428572]
    [0,WrappedArray(today, is, a, good, day),3,WrappedArray(I, am, not, a, boy),0.8888888888888888]
    [5,WrappedArray(mom, I, love, you),3,WrappedArray(I, am, not, a, boy),0.875]
    [4,WrappedArray(today, is, not, a, good, day),0,WrappedArray(today, is, a, good, day),0.16666666666666663]
    [2,WrappedArray(I, am, a, girl),0,WrappedArray(today, is, a, good, day),0.875]
    [2,WrappedArray(I, am, a, girl),3,WrappedArray(I, am, not, a, boy),0.5]
    [3,WrappedArray(I, am, not, a, boy),3,WrappedArray(I, am, not, a, boy),0.0]
    [0,WrappedArray(today, is, a, good, day),4,WrappedArray(today, is, not, a, good, day),0.16666666666666663]
    [3,WrappedArray(I, am, not, a, boy),0,WrappedArray(today, is, a, good, day),0.8888888888888888]
    [4,WrappedArray(today, is, not, a, good, day),4,WrappedArray(today, is, not, a, good, day),0.0]
    [3,WrappedArray(I, am, not, a, boy),2,WrappedArray(I, am, a, girl),0.5]
    [2,WrappedArray(I, am, a, girl),2,WrappedArray(I, am, a, girl),0.0]
    [3,WrappedArray(I, am, not, a, boy),5,WrappedArray(mom, I, love, you),0.875]
    [2,WrappedArray(I, am, a, girl),4,WrappedArray(today, is, not, a, good, day),0.8888888888888888]
    [3,WrappedArray(I, am, not, a, boy),4,WrappedArray(today, is, not, a, good, day),0.7777777777777778]
    [1,WrappedArray(hello, word),1,WrappedArray(hello, word),0.0]
    [0,WrappedArray(today, is, a, good, day),0,WrappedArray(today, is, a, good, day),0.0]
    [4,WrappedArray(today, is, not, a, good, day),2,WrappedArray(I, am, a, girl),0.8888888888888888]
    [5,WrappedArray(mom, I, love, you),2,WrappedArray(I, am, a, girl),0.8571428571428572]
     */

    val colRenamed = Seq("id1", "segWords1", "id2", "segWords2", "distCol")
    val df3 = df1.select("datasetA.id", "datasetA.segWords", "datasetB.id", "datasetB.segWords", "distCol").
      toDF(colRenamed: _*)
    //    df3.printSchema()
    /*
    root
 |-- id1: long (nullable = true)
 |-- segWords1: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- id2: long (nullable = true)
 |-- segWords2: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- distCol: double (nullable = true)
     */


    sc.stop()
    spark.stop()

  }
}
