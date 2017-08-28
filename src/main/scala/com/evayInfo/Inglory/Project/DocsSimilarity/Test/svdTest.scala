package com.evayInfo.Inglory.Project.DocsSimilarity.Test

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector => MLLibVector, Vectors}
import org.apache.spark.sql.SparkSession

object svdTest {

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

    val documents = sc.textFile("file:///D:\\Workspace\\IDEA\\GitHub\\SparkDiary\\data\\documents.txt").map(_.split(" ").toSeq).
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
//   println(termIds.mkString(";"))
    /*
    a;I;is;am;not;good;day!;today;love;hello;girl.;you.;boy.;mom,;word!
     */
    docTermFreqs.cache()

//        val docIds = docTermFreqs.rdd.map(_.getString(0)).zipWithUniqueId().map(_.swap).collect().toMap
    val docIds = docTermFreqs.rdd.map(_.getLong(1)).zipWithUniqueId().map(_.swap).collect().toMap
//    println(docIds)
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
    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row =>
      Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }
//    vecRdd.collect().foreach(println)
    /*
(15,[0,2,5,6,7],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])
(15,[0,1,3,10],[0.3364722366212129,0.5596157879354227,0.8472978603872037,1.252762968495368])
(15,[0,2,4,5,6,7],[0.3364722366212129,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037,0.8472978603872037])
(15,[9,14],[1.252762968495368,1.252762968495368])
(15,[0,1,3,4,12],[0.3364722366212129,0.5596157879354227,0.8472978603872037,0.8472978603872037,1.252762968495368])
(15,[1,8,11,13],[0.5596157879354227,1.252762968495368,1.252762968495368,1.252762968495368])
     */
    vecRdd.cache()

    val mat = new RowMatrix(vecRdd)
    val k = 2
    val svd = mat.computeSVD(k, computeU=true)

    val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
//    println(VS)
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
//    println(normalizedVS)
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


    val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
//    US.rows.collect().foreach(println)
    /*
[1.5800705145061005,0.3064207344096374]
[0.3216871059177372,-0.5017744377290927]
[1.8583363189877355,0.2655462649192283]
[-2.6163276725850184E-16,5.264435702225573E-12]
[0.6873258316656085,-0.5604201433914167]
[0.20435618918084833,-2.1092340282790403]
     */


    val normalizedUS: RowMatrix = distributedRowsNormalized(US)
//    normalizedUS.rows.collect().foreach(println)
/*
[0.9817101645202613,0.1903815980540171]
[0.5397099037726213,-0.8418510674518076]
[0.9899442987459717,0.14145771587420197]
[-4.9698159828372614E-5,0.9999999987650464]
[0.7750268420897188,-0.6319283140043956]
[0.09643488706359037,-0.9953392951938713]
 */

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


    sc.stop()
    spark.stop()
  }

  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: MLLibVector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  /**
   * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
   * Breeze doesn't support efficient diagonal representations, so multiply manually.
   */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: MLLibVector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs { case ((r, c), v) => v * sArr(c) }
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
