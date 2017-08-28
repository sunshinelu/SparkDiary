package com.evayInfo.Inglory.Project.DocsSimilarity.Test


import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import com.evayInfo.Inglory.Project.DocsSimilarity.{DocsimiCountVectorizer, DocsimiUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector => MLLibVector, Vectors}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}


/**
 * Created by sunlu on 17/8/28.
 *
 * 使用RDD保存相似性结果
 *
 * spark-shell 测试成功！
 *
 */
object DocsimiSVDTest1 {

  case class DocsimiIdSchema(doc1Id: Long, doc2Id: Long, value: Double)

  def main(args: Array[String]): Unit = {

    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiSVDTest1") //.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._


    val ylzxTable = args(0)
    val docSimiTable = args(1)

    /*
        val ylzxTable =  "yilan-total_webpage"
        val docSimiTable = "docsimi_svd"
    */

    val ylzxRDD = DocsimiCountVectorizer.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).randomSplit(Array(0.01, 0.99))(0)

    val vocabSize: Int = 20000

    val vocabModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("segWords").
      setOutputCol("features").
      setVocabSize(vocabSize).
      setMinDF(2).
      fit(ylzxDS)

    val docTermFreqs = vocabModel.transform(ylzxDS)

    val doc2IdLab = docTermFreqs.select("id", "urlID", "title", "label", "websitename", "time").withColumnRenamed("id", "doc2Id").withColumnRenamed("urlID", "doc2")
    val doc1IdLab = docTermFreqs.select("id", "urlID").withColumnRenamed("id", "doc1Id").withColumnRenamed("urlID", "doc1")

    docTermFreqs.cache()

    val idf = new IDF().setInputCol("features").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs) //.select("id", "tfidfVec")

    docTermMatrix.cache()

    val vecRdd = docTermMatrix.select("id", "tfidfVec").na.drop.rdd.map {
      case Row(id: Long, features: MLVector) => (id.toLong, Vectors.fromML(features))
    }

    vecRdd.cache()
    val indexedRdd = vecRdd.map { case (i, v) => new IndexedRow(i, v) }
    val mat = new IndexedRowMatrix(indexedRdd)

    val k = 10
    val svd = mat.computeSVD(k, computeU = true)

    val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)

    val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)

    val US: IndexedRowMatrix = multiplyByDiagonalIndexedRowMatrix(svd.U, svd.s)
    val normalizedUS: IndexedRowMatrix = distributedIndexedRowNormalized(US)

    val docVec = normalizedUS.rows.map { vec => {
      val index = vec.index
      val docRowArr = vec.vector.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      (index, docRowVec)
    }
    }
    val indexList = docVec.map(_._1).collect().toList
    val docSimi = indexList.flatMap(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id = docScores.map(_.index).collect().toList
      val score = docScores.map(_.vector.apply(0).toDouble).collect().toList
      val zipD = id.zip(score).map(y => {
        DocsimiIdSchema(x, y._1, y._2)
      }
      ).filter(x => {
        x.value >= 0.5 & x.value <= 0.999
      }) //.map(x => {(x._1 + ";" + x._2 + ";" + x._3)})
      zipD

    })

    val docSimiRDD = sc.parallelize(docSimi)

    val docsimiDS = spark.createDataset(docSimiRDD)
    val ds1 = docsimiDS.join(doc1IdLab, Seq("doc1Id"), "left")
    val ds2 = ds1.join(doc2IdLab, Seq("doc2Id"), "left")


    //对dataframe进行分组排序，并取每组的前5个
    val w = Window.partitionBy("doc1Id").orderBy(col("value").desc)
    val ds3 = ds2.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    val ds4 = ds3.select("doc1", "doc2", "value", "rn", "title", "label", "time", "websitename")

    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    //    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

    /*
    //如果outputTable存在则不做任何操作，如果HBASE表不存在则新建表
    val hadmin = new HBaseAdmin(hbaseConf)
    if (!hadmin.isTableAvailable(docSimiTable)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(TableName.valueOf(docSimiTable))
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes()))
//      tableDesc.addFamily(new HColumnDescriptor("f".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
*/

    //如果outputTable表存在，则删除表；如果不存在则新建表。=> START
    val hAdmin = new HBaseAdmin(hbaseConf)
    if (hAdmin.tableExists(docSimiTable)) {
      hAdmin.disableTable(docSimiTable)
      hAdmin.deleteTable(docSimiTable)
    }
    //    val htd = new HTableDescriptor(outputTable)
    val htd = new HTableDescriptor(TableName.valueOf(docSimiTable))
    htd.addFamily(new HColumnDescriptor("info".getBytes()))
    hAdmin.createTable(htd)
    //如果outputTable表存在，则删除表；如果不存在则新建表。=> OVER

    //指定输出格式和输出表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, docSimiTable) //设置输出表名


    //    val table = new HTable(hbaseConf,docSimiTable)
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,docSimiTable)

    val jobConf = new Configuration(hbaseConf)
    jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    ds4.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map { x => {
        //("doc1","doc2", "value", "rn","title","label","time","websitename")
        val paste = x._1.toString + "::score=" + x._4.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //doc1
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._2.toString)) //doc2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._3.toString)) //value
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._4.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._5.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._7.toString)) //time2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._8.toString)) //websitename2

        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()

  }

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

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalIndexedRowMatrix(mat: IndexedRowMatrix, diag: MLLibVector): IndexedRowMatrix = {
    val indexedRow = mat.rows
    val sArr = diag.toArray
    new IndexedRowMatrix(indexedRow.map { vec =>
      val indxed = vec.index.toLong
      val vecArr = vec.vector.toArray
      val newArr = (0 until vec.vector.size).toArray.map(i => vecArr(i) * sArr(i))
      new IndexedRow(indxed, Vectors.dense(newArr))
    })
  }


  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def distributedIndexedRowNormalized(mat: IndexedRowMatrix): IndexedRowMatrix = {
    val indexedRow = mat.rows
    new IndexedRowMatrix(indexedRow.map { vec =>
      val indxed = vec.index.toLong
      val array = vec.vector.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      new IndexedRow(indxed, Vectors.dense(array.map(_ / length)))
    })
  }


}
