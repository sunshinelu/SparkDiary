package com.evayInfo.Inglory.Project.DocsSimilarity

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by sunlu on 17/8/23.
  */
object DocsimiALS {

  case class docSimsSchema(doc1: String, doc2: String, sims: Double)

  def main(args: Array[String]): Unit = {
    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiALS")//.setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val ylzxTable = "yilan-total_webpage"//args(0)
    val logsTable = "t_hbaseSink"//args(1)
    val docSimiTable = "docsimi_als"//args(2)
*/

    val ylzxTable = args(0)
    val logsTable = args(1)
    val docSimiTable = args(2)

    val ylzxRDD = DocsimiUtil.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).dropDuplicates("content").drop("content")

    val logsRDD = DocsimiUtil.getLogsRDD(logsTable, sc)
    val logsDS = spark.createDataset(logsRDD).na.drop(Array("userString"))
    val ds1 = logsDS.groupBy("userString", "itemString").agg(sum("value")).withColumnRenamed("sum(value)", "rating")
    //string to number
    val userID = new StringIndexer().setInputCol("userString").setOutputCol("userID").fit(ds1)
    val ds2 = userID.transform(ds1)
    val urlID = new StringIndexer().setInputCol("itemString").setOutputCol("urlID").fit(ds2)
    val ds3 = urlID.transform(ds2)


    //Min-Max Normalization[-1,1]
    val minMax = ds3.agg(max("rating"), min("rating")).withColumnRenamed("max(rating)", "max").withColumnRenamed("min(rating)", "min")
    val maxValue = minMax.select("max").rdd.map { case Row(d: Double) => d }.first()
    val minValue = minMax.select("min").rdd.map { case Row(d: Double) => d }.first
    //limit the values to 4 digit
    val ds4 = ds3.withColumn("norm", bround((((ds3("rating") - minValue) / (maxValue - minValue)) * 2 - 1), 4))


    //RDD to RowRDD
    val alsRDD = ds4.select("userID", "urlID", "norm").rdd.
      map { case Row(userID: Double, urlID: Double, rating: Double) =>
        Rating(userID.toInt, urlID.toInt, rating)
      }
    // Build the recommendation model using ALS
    val rank = 10
    val numIterations = 10
    val alsModel = ALS.train(alsRDD, rank, numIterations, 0.01)

    val userFeatures = alsModel.productFeatures.map {
      case (id: Int, vec: Array[Double]) => {
        val vector = Vectors.dense(vec)
        new IndexedRow(id, vector)
      }
    }

    /* The minimum cosine similarity threshold for each document pair */
    val threshhold = 0.5.toDouble
    val upper = 1.0

    val mat = new IndexedRowMatrix(userFeatures)
    val mat_t = mat.toCoordinateMatrix.transpose()
    val sim = mat_t.toRowMatrix.columnSimilarities(threshhold)
    val sim2 = sim.entries.map { case MatrixEntry(i, j, u) => (i, j, u) }

    val sim_threshhold = sim.entries.filter { case MatrixEntry(i, j, u) => u >= threshhold && u <= upper }
//    println("item-item similarity is: ")
//    sim_threshhold.collect().foreach(println)

    val sim_threshhold2 = sim_threshhold.map { case MatrixEntry(i, j, u) => MatrixEntry(j, i, u) }
    val docsim = sim_threshhold.union(sim_threshhold2)

    val docSimsRDD = docsim.map { x => {
      val doc1 = x.i.toString
      val doc2 = x.j.toString
      val sims = x.value
      docSimsSchema(doc1, doc2, sims)
    }
    }
    val docSimsDS = spark.createDataset(docSimsRDD)

//    println("ds4 is: ")
//    ds4.printSchema()
/*
ds4 is:
root
 |-- userString: string (nullable = true)
 |-- itemString: string (nullable = true)
 |-- rating: double (nullable = true)
 |-- userID: double (nullable = true)
 |-- urlID: double (nullable = true)
 |-- norm: double (nullable = true)
 */
    val itemLab = ds4.select("itemString", "urlID").dropDuplicates
    val doc1Lab = itemLab.withColumnRenamed("urlID","doc1")
    val doc2Lab = itemLab.withColumnRenamed("urlID","doc2").withColumnRenamed("itemString", "url2id")

    val docSimsDS2 = docSimsDS.join(doc1Lab, Seq("doc1"), "left").join(doc2Lab, Seq("doc2"), "left")

    val wordsUrlLabDF = ylzxDS.withColumnRenamed("itemString", "url2id").
      select("url2id", "title", "manuallabel", "time", "websitename")

    val ds5 = docSimsDS2.join(wordsUrlLabDF, Seq("url2id"), "left")
//    println("ds5 is: ")
//    ds5.printSchema()
    /*
    ds5 is:
root
 |-- url2id: string (nullable = true)
 |-- doc2: string (nullable = true)
 |-- doc1: string (nullable = true)
 |-- sims: double (nullable = true)
 |-- itemString: string (nullable = true)
 |-- title: string (nullable = true)
 |-- manuallabel: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
     */
    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1").orderBy(col("sims").desc)
    val ds6 = ds5.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)
//    println("ds6 is: ")
//    ds6.printSchema()
/*
ds6 is:
root
 |-- url2id: string (nullable = true)
 |-- doc2: string (nullable = true)
 |-- doc1: string (nullable = true)
 |-- sims: double (nullable = true)
 |-- itemString: string (nullable = true)
 |-- title: string (nullable = true)
 |-- manuallabel: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- rn: integer (nullable = true)
 */
//    sims, rn, url2id,  title, manuallabel, time, websitename, urlID

    val ds7 = ds6.select("itemString","url2id","rn","sims","title","manuallabel","websitename","time").na.drop()


    val hbaseConf = HBaseConfiguration.create() //在HBaseConfiguration设置可以将扫描限制到部分列，以及限制扫描的时间范围
    //设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, ylzxTable) //设置输入表名

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


    ds7.rdd.map(row => (row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))).
      map { x => {
        //("itemString","url2id","rn","sims","title","manuallabel","websitename","time")
        val paste = x._1 + "::score=" + x._3.toString
        val key = Bytes.toBytes(paste)
        val put = new Put(key)
        put.add(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(x._1.toString)) //urlID
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsID"), Bytes.toBytes(x._2.toString)) //url2id
        put.add(Bytes.toBytes("info"), Bytes.toBytes("level"), Bytes.toBytes(x._3.toString)) //rn
        put.add(Bytes.toBytes("info"), Bytes.toBytes("simsScore"), Bytes.toBytes(x._4.toString)) //标签的family:qualify,sims
        put.add(Bytes.toBytes("info"), Bytes.toBytes("t"), Bytes.toBytes(x._5.toString)) //title2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("manuallabel"), Bytes.toBytes(x._6.toString)) //label2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("websitename"), Bytes.toBytes(x._7.toString)) //websitename2
        put.add(Bytes.toBytes("info"), Bytes.toBytes("mod"), Bytes.toBytes(x._8.toString)) //time2
        (new ImmutableBytesWritable, put)
      }
      }.saveAsNewAPIHadoopDataset(jobConf)


    sc.stop()
    spark.stop()
  }
}
