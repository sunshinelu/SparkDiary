package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import java.util

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector => MLLibVector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by sunlu on 17/8/26.
 * 是哟功能测试数据构建SVD模型计算文本相似性
 */

object svdTest2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]): Unit = {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"svdTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //    val documents = sc.textFile("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\documents.txt").map(_.split(" ").toSeq).
    //      zipWithUniqueId().toDF("segWords", "id")

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

    //    docTermFreqs.show()
    /*
+--------------------+---+--------------------+
|            segWords| id|            features|
+--------------------+---+--------------------+
|[today, is, a, go...|  0|(15,[0,3,4,6,7],[...|
|   [I, am, a, girl.]|  2|(15,[0,1,5,10],[1...|
|[today, is, not, ...|  4|(15,[0,2,3,4,6,7]...|
|      [hello, word!]|  1|(15,[11,14],[1.0,...|
|[I, am, not, a, b...|  3|(15,[0,1,2,5,13],...|
|[mom,, I, love, y...|  5|(15,[1,8,9,12],[1...|
+--------------------+---+--------------------+
     */

    val termIds = vocabModel.vocabulary
//       println(termIds.mkString(";"))
    /*
a;I;day;am;today;is;good;not;girl;love;word;boy;mom;hello;you
     */
    docTermFreqs.cache()

    //        val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap
    val docIds = docTermFreqs.rdd.map(_.getLong(1)).zipWithUniqueId().map(_.swap).collect().toMap
//        println(docIds)
    /*
    Map(0 -> 0, 5 -> 5, 1 -> 1, 2 -> 2, 3 -> 3, 4 -> 4)
     */

    val idf = new IDF().setInputCol("features").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs).select("id", "tfidfVec")
    //    docTermMatrix.show()
    /*
    +---+--------------------+
    | id|            tfidfVec|
    +---+--------------------+
    |  0|(15,[0,3,4,6,7],[...|
    |  2|(15,[0,1,5,10],[0...|
    |  4|(15,[0,2,3,4,6,7]...|
    |  1|(15,[11,14],[1.25...|
    |  3|(15,[0,1,2,5,13],...|
    |  5|(15,[1,8,9,12],[0...|
    +---+--------------------+

     */
    docTermMatrix.cache()
    val vecRdd = docTermMatrix.select("id", "tfidfVec").na.drop.rdd.map {
      case Row(id: Long, features: MLVector) => (id.toLong, Vectors.fromML(features))
    }
//        vecRdd.collect().foreach(println)
    /*
(0,(15,[0,2,3,4,5],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]))
(2,(15,[0,1,7,9],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368]))
(4,(15,[0,2,3,4,5,6],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037]))
(1,(15,[12,13],[1.252762968495368,1.252762968495368]))
(3,(15,[0,1,6,7,14],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368]))
(5,(15,[1,8,10,11],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368]))
     */
    vecRdd.cache()
   val indexedRdd = vecRdd.map { case (i, v) => new IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(indexedRdd)

    val k = 2
    val svd = mat.computeSVD(k, computeU=true)
   val V_M =  svd.V
//    println("V_M is: ")
//    println(V_M)
    /*
0.22784747106595502      -0.0318486174801178
0.10338790949607693      -0.34267980052846625
0.4435884464587864       0.09357302926486694
0.44358844645878653      0.09357302926496347
0.4435884464587848       0.09357302928117989
0.4435884464587856       0.09357302927463745
0.3284155637481148       -0.048240968614897795
0.13017263608233637      -0.17377360013402027
0.03898015464733362      -0.5101962754685542
0.06136057433342263      -0.12137270962539493
0.03898015464733311      -0.5101962754635345
0.03898015464733298      -0.5101962754599635
-1.8205956772195797E-16  3.0515101347828755E-12
-2.678502126899547E-17   1.1507498574233335E-12
0.13110475057705454      -0.13555834299645386
     */
    val U_M = svd.U
//    println("U_M is: ")
//    U_M.rows.collect().foreach(println)
    /*
IndexedRow(0,[0.6165522634121898,0.13464491387104627])
IndexedRow(2,[0.12552408987018784,-0.2204856537560777])
IndexedRow(4,[0.7251331210437203,0.11668418600233925])
IndexedRow(1,[-1.0209055441202665E-16,2.3132556387591814E-12])
IndexedRow(3,[0.2681983451526907,-0.24625527408880016])
IndexedRow(5,[0.07974091651291,-0.926822509640781])
     */
    val s_M = svd.s
//    println("s_M is: ")
//    println(s_M)
/*
[2.562751948653151,2.275769099626788]
 */


       val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
//           println(VS)
       /*
-0.5839165504699695      -0.07248009955572628
-0.26495756652824626     -0.7798601011118605
-1.1368071555622723      0.21295060856508935
-0.33360017678132076     -0.39546858953534997
-1.1368071555622743      0.2129506085721032
-1.1368071555622734      0.2129506085692902
-1.1368071555622765      0.21295060857740392
-0.84164762596351        -0.10978530567471534
-0.1572519314434444      -0.276216262160033
-0.09989646728125595     -1.1610889184434448
-1.3416828843769847E-15  5.1875220926613914E-12
-0.335988955019041       -0.3084994881333885
-0.09989646728125538     -1.1610889184462783
1.6837265138571055E-15   -4.819069720231736E-12
-0.0998964672812562      -1.1610889184438056
        */

           val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
//               println(normalizedVS)
           /*
-0.9923840776891519     -0.12318215109767908
-0.3216906480527493     -0.9468448272844934
-0.98290360944556       0.1841208693736
-0.6447846254871572     -0.7643643023685672
-0.9829036094444625     0.1841208693794584
-0.9829036094449026     0.1841208693771088
-0.9829036094436331     0.18412086938388575
-0.9915996283719541     -0.12934518550221508
-0.4947486093742629     -0.8690361405155905
-0.08572019916391395    -0.9963192497665089
-2.5863655431954856E-4  0.9999999665535657
-0.7365973795688988     -0.676331501862978
-0.08572019916370581    -0.9963192497665266
3.4938826554434455E-4   -0.999999938963918
-0.08572019916388772    -0.9963192497665111
            */


           val US: IndexedRowMatrix = multiplyByDiagonalIndexedRowMatrix(svd.U, svd.s)
//               US.rows.collect().foreach(println)
           /*
IndexedRow(0,[1.5800705145061005,0.3064207344096374])
IndexedRow(2,[0.3216871059177372,-0.5017744377290927])
IndexedRow(4,[1.8583363189877355,0.2655462649192283])
IndexedRow(1,[-2.6163276725850184E-16,5.264435702225573E-12])
IndexedRow(3,[0.6873258316656085,-0.5604201433914167])
IndexedRow(5,[0.20435618918084833,-2.1092340282790403])
            */


               val normalizedUS: IndexedRowMatrix = distributedIndexedRowNormalized(US)
//                   normalizedUS.rows.collect().foreach(println)
               /*
IndexedRow(0,[-0.9817101645213376,0.19038159804846747])
IndexedRow(2,[-0.5397099037395773,-0.8418510674729921])
IndexedRow(4,[-0.9899442987456872,0.14145771587619227])
IndexedRow(1,[8.243689491674831E-4,0.99999966020786])
IndexedRow(3,[-0.7750268421067975,-0.6319283139834495])
IndexedRow(5,[-0.09643488706364706,-0.9953392951938659])

                */

   val docVec =  normalizedUS.rows.map{vec => {
      val index = vec.index
      val docRowArr = vec.vector.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      // Compute scores against every doc
//      val docScores = normalizedUS.multiply(docRowVec).rows
      // Find the docs with the highest scores
//      val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId//.map(_.vector)
//      val simiIndex = docScores.rows.map(_.index)

      (index,docRowVec)
    }}

//    docVec.collect().foreach(println)
    /*
(0,-0.9817101645213376  0.19038159804846747  )
(2,-0.5397099037395773  -0.8418510674729921  )
(4,-0.9899442987456872  0.14145771587619227  )
(1,8.243689491674831E-4  0.99999966020786      )
(3,-0.7750268421067975  -0.6319283139834495  )
(5,-0.09643488706364706  -0.9953392951938659   )
     */
    val indexList = docVec.map(_._1).collect().toList
    for (i <- indexList) {
     val docRowArr = docVec.lookup(i).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec)
      println(i)
      docScores.rows.collect().foreach(println)
      println("====")
    }
    /*
0
IndexedRow(0,[1.0])
IndexedRow(2,[0.36956574684965926])
IndexedRow(4,[0.9987693263933843])
IndexedRow(1,[0.18957224198158337])
IndexedRow(3,[0.6405442064048741])
IndexedRow(5,[-0.09482317677459391])
====
2
IndexedRow(0,[0.36956574684965926])
IndexedRow(2,[1.0])
IndexedRow(4,[0.4151964130709145])
IndexedRow(1,[-0.8422957015048175])
IndexedRow(3,[0.9502791880424233])
IndexedRow(5,[0.8899743117710295])
====
4
IndexedRow(0,[0.9987693263933843])
IndexedRow(2,[0.4151964130709145])
IndexedRow(4,[1.0])
IndexedRow(1,[0.14064158846868094])
IndexedRow(3,[0.6778422678249061])
IndexedRow(5,[-0.04533325657110171])
====
1
IndexedRow(0,[0.18957224198158337])
IndexedRow(2,[-0.8422957015048175])
IndexedRow(4,[0.14064158846868094])
IndexedRow(1,[0.9999999999999999])
IndexedRow(3,[-0.6325670073225795])
IndexedRow(5,[-0.9954184549119085])
====
3
IndexedRow(0,[0.6405442064048741])
IndexedRow(2,[0.9502791880424233])
IndexedRow(4,[0.6778422678249061])
IndexedRow(1,[-0.6325670073225795])
IndexedRow(3,[1.0])
IndexedRow(5,[0.7037227086431986])
====
5
IndexedRow(0,[-0.09482317677459391])
IndexedRow(2,[0.8899743117710295])
IndexedRow(4,[-0.04533325657110171])
IndexedRow(1,[-0.9954184549119085])
IndexedRow(3,[0.7037227086431986])
IndexedRow(5,[1.0])
====
     */

    val result = new util.ArrayList[java.util.Map[Long,List[IndexedRow]]]

    indexList.foreach(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows.collect().toList
      var put = new util.HashMap[Long, List[IndexedRow]]()
      put.put(x, docScores)
      result.add(put)
    })
    println(result)
    /*
    [{0=List(IndexedRow(0,[1.0]), IndexedRow(2,[0.36956574684965926]), IndexedRow(4,[0.9987693263933843]), IndexedRow(1,[0.18957224198158337]), IndexedRow(3,[0.6405442064048741]), IndexedRow(5,[-0.09482317677459391]))},
    {2=List(IndexedRow(0,[0.36956574684965926]), IndexedRow(2,[1.0]), IndexedRow(4,[0.4151964130709145]), IndexedRow(1,[-0.8422957015048175]), IndexedRow(3,[0.9502791880424233]), IndexedRow(5,[0.8899743117710295]))},
    {4=List(IndexedRow(0,[0.9987693263933843]), IndexedRow(2,[0.4151964130709145]), IndexedRow(4,[1.0]), IndexedRow(1,[0.14064158846868094]), IndexedRow(3,[0.6778422678249061]), IndexedRow(5,[-0.04533325657110171]))},
    {1=List(IndexedRow(0,[0.18957224198158337]), IndexedRow(2,[-0.8422957015048175]), IndexedRow(4,[0.14064158846868094]), IndexedRow(1,[0.9999999999999999]), IndexedRow(3,[-0.6325670073225795]), IndexedRow(5,[-0.9954184549119085]))},
    {3=List(IndexedRow(0,[0.6405442064048741]), IndexedRow(2,[0.9502791880424233]), IndexedRow(4,[0.6778422678249061]), IndexedRow(1,[-0.6325670073225795]), IndexedRow(3,[1.0]), IndexedRow(5,[0.7037227086431986]))},
     {5=List(IndexedRow(0,[-0.09482317677459391]), IndexedRow(2,[0.8899743117710295]), IndexedRow(4,[-0.04533325657110171]), IndexedRow(1,[-0.9954184549119085]), IndexedRow(3,[0.7037227086431986]), IndexedRow(5,[1.0]))}]

     */


    val result2 = new util.ArrayList[java.util.Map[Long,String]]

    indexList.foreach(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id  = docScores.map(x => {(x.index + ":" + x.vector.toString)}).collect().mkString(";")

      var put = new util.HashMap[Long, String]()
      put.put(x, id)
      result2.add(put)
    })
    println("result2 is: ")
    println(result2)
    /*
    [{0=0:[1.0];2:[0.36956574684965926];4:[0.9987693263933843];1:[0.18957224198158337];3:[0.6405442064048741];5:[-0.09482317677459391]},
    {2=0:[0.36956574684965926];2:[1.0];4:[0.4151964130709145];1:[-0.8422957015048175];3:[0.9502791880424233];5:[0.8899743117710295]},
    {4=0:[0.9987693263933843];2:[0.4151964130709145];4:[1.0];1:[0.14064158846868094];3:[0.6778422678249061];5:[-0.04533325657110171]},
    {1=0:[0.18957224198158337];2:[-0.8422957015048175];4:[0.14064158846868094];1:[0.9999999999999999];3:[-0.6325670073225795];5:[-0.9954184549119085]},
    {3=0:[0.6405442064048741];2:[0.9502791880424233];4:[0.6778422678249061];1:[-0.6325670073225795];3:[1.0];5:[0.7037227086431986]},
    {5=0:[-0.09482317677459391];2:[0.8899743117710295];4:[-0.04533325657110171];1:[-0.9954184549119085];3:[0.7037227086431986];5:[1.0]}]

     */

    val result2RDD = sc.parallelize(result2.toArray())
    println("result2RDD is: ")
    result2RDD.collect().foreach(println)
/*
{0=0:[1.0];2:[0.36956574684965926];4:[0.9987693263933843];1:[0.18957224198158337];3:[0.6405442064048741];5:[-0.09482317677459391]}
{2=0:[0.36956574684965926];2:[1.0];4:[0.4151964130709145];1:[-0.8422957015048175];3:[0.9502791880424233];5:[0.8899743117710295]}
{4=0:[0.9987693263933843];2:[0.4151964130709145];4:[1.0];1:[0.14064158846868094];3:[0.6778422678249061];5:[-0.04533325657110171]}
{1=0:[0.18957224198158337];2:[-0.8422957015048175];4:[0.14064158846868094];1:[0.9999999999999999];3:[-0.6325670073225795];5:[-0.9954184549119085]}
{3=0:[0.6405442064048741];2:[0.9502791880424233];4:[0.6778422678249061];1:[-0.6325670073225795];3:[1.0];5:[0.7037227086431986]}
{5=0:[-0.09482317677459391];2:[0.8899743117710295];4:[-0.04533325657110171];1:[-0.9954184549119085];3:[0.7037227086431986];5:[1.0]}

 */



    val result3 = new util.ArrayList[java.util.Map[Long,String]]

    indexList.foreach(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id  = docScores.map(_.index)
      val score = docScores.map(_.vector.apply(0).toDouble)
      val zipD = id.zip(score).top(3).foreach( y => {
        var put = new util.HashMap[Long, String]()
        put.put(x,y._1.toString + ";" + y._2.toString)
        result3.add(put)
      }
      )


    })
    println("result3 is: ")
    /*
    [{0=5;-0.09482317678027817},
     {0=4;0.9987693263930042},
     {0=3;0.6405442063797788},
     {2=5;0.8899743117531044},
     {2=4;0.415196413108452},
     {2=3;0.9502791880630621},
     {4=5;-0.04533325656915016},
     {4=4;1.0},
     {4=3;0.6778422678065144},
     {1=5;-0.9953440866011037},
     {1=4;0.141408517289528},
     {1=3;-0.6319668306318629},
     {3=5;0.7037227086623596},
     {3=4;0.6778422678065144},
     {3=3;1.0},
     {5=5;0.9999999999999999},
     {5=4;-0.04533325656915016},
     {5=3;0.7037227086623596}]

     */
    println(result3)
sc.parallelize(result3.toArray).collect().foreach(println)
/*
{0=5;-0.09482317678027817}
{0=4;0.9987693263930042}
{0=3;0.6405442063797788}
{2=5;0.8899743117531044}
{2=4;0.415196413108452}
{2=3;0.9502791880630621}
{4=5;-0.04533325656915016}
{4=4;1.0}
{4=3;0.6778422678065144}
{1=5;-0.9953440866011037}
{1=4;0.141408517289528}
{1=3;-0.6319668306318629}
{3=5;0.7037227086623596}
{3=4;0.6778422678065144}
{3=3;1.0}
{5=5;0.9999999999999999}
{5=4;-0.04533325656915016}
{5=3;0.7037227086623596}
 */

// result4 is the best one! for now....
    val result4 = new ArrayBuffer[String]()

    indexList.foreach(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id  = docScores.map(_.index)
      val score = docScores.map(_.vector.apply(0).toDouble)
      val zipD = id.zip(score).top(3).foreach( y => {
        result4 ++= Array(x + ";" + y._1 + ";" + y._2)

      }
      )


    })
    println("result4 is: ")
    println(result4)
/*
ArrayBuffer(0;5;-0.09482317677459391, 0;4;0.9987693263933843, 0;3;0.6405442064048741, 2;5;0.8899743117710295, 2;4;0.4151964130709145, 2;3;0.9502791880424233, 4;5;-0.04533325657110171, 4;4;1.0, 4;3;0.6778422678249061, 1;5;-0.9954184549119085, 1;4;0.14064158846868094, 1;3;-0.6325670073225795, 3;5;0.7037227086431986, 3;4;0.6778422678249061, 3;3;1.0, 5;5;1.0, 5;4;-0.04533325657110171, 5;3;0.7037227086431986)

 */
    val result4RDD = sc.parallelize(result4).map(_.split(";")).map(x => {
      val id = x(0)
      val simiId = x(1)
      val simiScore = x(2)
      (id, simiId,simiScore )
    })
    result4RDD.collect().foreach(println)
/*
(0,5,-0.09482317677459391)
(0,4,0.9987693263933843)
(0,3,0.6405442064048741)
(2,5,0.8899743117710295)
(2,4,0.4151964130709145)
(2,3,0.9502791880424233)
(4,5,-0.04533325657110171)
(4,4,1.0)
(4,3,0.6778422678249061)
(1,5,-0.9954184549119085)
(1,4,0.14064158846868094)
(1,3,-0.6325670073225795)
(3,5,0.7037227086431986)
(3,4,0.6778422678249061)
(3,3,1.0)
(5,5,1.0)
(5,4,-0.04533325657110171)
(5,3,0.7037227086431986)

 */


    val indexList2 = docVec.map(_._1)

    val mapTest = indexList.flatMap(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id = docScores.map(_.index).collect().toList
      val score = docScores.map(_.vector.apply(0).toDouble).collect().toList
      val zipD = id.zip(score).map(y => {
        (x, y._1, y._2)
      }
      ).filter(_._3 >= 0.5) //.map(x => {(x._1 + ";" + x._2 + ";" + x._3)})
      zipD

    })
    println("=======")
    println(mapTest)
    val mapTestRDD = sc.parallelize(mapTest)
    mapTestRDD.collect().foreach(println)


    val mapTest2 = indexList2.flatMap(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id = docScores.map(_.index).collect().toList
      val score = docScores.map(_.vector.apply(0).toDouble).collect().toList
      val zipD = id.zip(score).map(y => {
        (x, y._1, y._2)
      }
      ).filter(_._3 >= 0.5) //.map(x => {(x._1 + ";" + x._2 + ";" + x._3)})
      zipD

    })
    println("=====mapTest2======")
    mapTest2.collect().foreach(println)


    val test = for (i <- indexList) {
      val docRowArr = docVec.lookup(i).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows.collect().toList

    }

//    val docVecMap = docVec.lookup(1)


    /*
               def topDocsForDoc(docId: Long): Seq[(Double, Long)] = {
                 // Look up the row in US corresponding to the given doc ID.
                 val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
                 val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

                 // Compute scores against every doc
                 val docScores = normalizedUS.multiply(docRowVec)

                 // Find the docs with the highest scores
                 val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

                 // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
                 allDocWeights.filter(!_._1.isNaN).top(10)
               }

               val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docIds(1L)).head.toArray



               val topDoc10 = docTermFreqs.rdd.map(_.getLong(1)).map(x => {
                 val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docIds(x)).head.toArray
                 val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
                 // Compute scores against every doc
                 val docScores = normalizedUS.multiply(docRowVec)

                 // Find the docs with the highest scores
                 val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
                 val doc10 = allDocWeights.filter(!_._1.isNaN).top(10)

                 //     val topDoc = topDocsForDoc(x)//.map{ case (score, id) => (docIds(id), score) }//.mkString(";")
                 (x, doc10)
               })
               topDoc10.collect().foreach(println)


               /*
               .flatMap(x => {
               val y = x._1
               for (x <- x._2)
             })


               .flatMap(x => {
               val y = x._3
               for (w <- y) yield (x._1,x._2, w)
             }).map(x=>(x._3, 1)).reduceByKey(_ + _).sortBy(_._2, false).take(35)

           */

               /*
                   val documents = sc.textFile("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\documents.txt").map(_.split(" ").toSeq).
                 zipWithUniqueId()

               val hashingTF = new HashingTF(Math.pow(2, 18).toInt)

               val tf = documents.map { x => {
                 val tf = hashingTF.transform(x._1)
                 (x._2, tf)
               }
               }
               tf.persist()

               //构建idf model
               val idf = new IDF().fit(tf.values)

               //将tf向量转换成tf-idf向量
               val tfidf = tf.mapValues(v => idf.transform(v))

               val tfidf_Indexed = tfidf.map { case (i, v) => new IndexedRow(i, v) }

               val indexed_matrix = new IndexedRowMatrix(tfidf_Indexed)

               val k = 100
               val svd = indexed_matrix.computeSVD(k, computeU=true)
               val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
                */

*/
    sc.stop()
    spark.stop()
  }

  /**
    * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
    */
  def multiplyByDiagonalIndexedRowMatrix(mat: IndexedRowMatrix, diag: MLLibVector): IndexedRowMatrix = {
    val indexedRow  = mat.rows
    val sArr = diag.toArray
    new IndexedRowMatrix(indexedRow.map { vec =>
      val indxed = vec.index.toLong
      val vecArr = vec.vector.toArray
      val newArr = (0 until vec.vector.size).toArray.map(i => vecArr(i) * sArr(i))
      new IndexedRow (indxed, Vectors.dense(newArr))
    })
  }

  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
  }

  /**
    * Returns a distributed matrix where each row is divided by its length.
    */
  def distributedIndexedRowNormalized(mat: IndexedRowMatrix): IndexedRowMatrix = {
    val indexedRow  = mat.rows
    new IndexedRowMatrix(indexedRow.map { vec =>
      val indxed = vec.index.toLong
      val array = vec.vector.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      new IndexedRow (indxed,Vectors.dense(array.map(_ / length)))
    })
  }

  /**
   * Returns a matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).foreach(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

}
