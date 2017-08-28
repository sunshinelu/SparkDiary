package com.evayInfo.Inglory.Project.DocsSimilarity.Test


import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import com.evayInfo.Inglory.Project.DocsSimilarity.{DocsimiCountVectorizer, DocsimiUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector => MLLibVector, Vectors}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer


/**
 * Created by sunlu on 17/8/26.
 * 构建SVD模型计算文本相似性
 *
 * 使用ArrayBuffer保存相似性文件
 * spark-shell 测试成功！
 *
 */

/**
 * Created by sunlu on 17/8/28.
 */
object DocsimiSVDTest0 {

  case class DocsimiIdSchema(doc1Id: Long, doc2Id: Long, value: Double)

  def main(args: Array[String]): Unit = {

    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiSVD") //.setMaster("local[*]").set("spark.executor.memory", "2g")
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
    //    ylzxDS.printSchema()
    /*
    root
 |-- id: long (nullable = false)
 |-- urlID: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
     */
    //    println("ylzxDS的数量为：" + ylzxDS.count())

    val vocabSize: Int = 20000

    val vocabModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("segWords").
      setOutputCol("features").
      setVocabSize(vocabSize).
      setMinDF(2).
      fit(ylzxDS)

    val docTermFreqs = vocabModel.transform(ylzxDS)
    docTermFreqs.printSchema()
    /*
root
 |-- id: long (nullable = false)
 |-- urlID: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- features: vector (nullable = true)

     */

    /*
     docTermFreqs.show(5)
+----+--------------------+--------------------+-----+----------+-----------+--------------------+--------------------+
|  id|               urlID|               title|label|      time|websitename|            segWords|            features|
+----+--------------------+--------------------+-----+----------+-----------+--------------------+--------------------+
| 595|001fe90a-3f1c-491...|日照市机关事务管理局公务用车定点维...|  招投标|2017-08-09|   日照市政府采购网|[采购, 日照市, 事务, 管理局...|(8663,[1,2,3,5,6,...|
|1071|0043ec7c-1802-487...|金华市委副书记马小秋调研梅溪流域生...|   政务|2017-08-24|     浙江省水利厅|[金华, 市委, 书记, 马小秋,...|(8663,[2,4,5,8,9,...|
|1411|005f0d78-c8b7-48f...|  临沂：沂水县泉庄镇：万亩葡萄喜获丰收|   政务|2017-08-09|     山东省农业厅|[时节, 沂水, 1.5万亩, 优...|(8663,[1,4,9,10,1...|
|2380|00a319ef-8392-466...|关于报送2017年度林业工程高级专...|   人才|2017-08-21|      日照人社局|[各区, 人力, 资源, 社会, ...|(8663,[3,5,8,10,1...|
|4080|011f9d11-89f6-457...|        今年以来我省上市步伐加快|   政务|2017-06-24|福建省发展和改革委员会|[5月, 22日, 顶点, 软件,...|(8663,[48,67,80,1...|
+----+--------------------+--------------------+-----+----------+-----------+--------------------+--------------------+
     */
    val doc2IdLab = docTermFreqs.select("id", "urlID", "title", "label", "websitename", "time").withColumnRenamed("id", "doc2Id").withColumnRenamed("urlID", "doc2")
    val doc1IdLab = docTermFreqs.select("id", "urlID").withColumnRenamed("id", "doc1Id").withColumnRenamed("urlID", "doc1")

    //    val termIds = vocabModel.vocabulary
    /*
termIds: Array[String] = Array(项目, 采购, 发展, 2017年, 时间, 建设, 工作, 服务, 文件, 招标, 管理, 工程, 方式, 创新, 机构, 资源, 投标, 名称, 供应商, 提供, 经济, 山东, 地点, 单位, 人才, 科技, 有限公司, 报名, 资, 国家, 北京, 公共, 发布, 交易, 改革, 相关, 推进, 社会, 活动, 联系人, 数据, 投标人, 部门, 地址, 人员, 组, 创业, 信息, 生产, 山东省, 代理, 情况, 公告, 医疗, 1., 会议, 政策, 内容, 5月, 公司, 济南市, 合作, 实施, 系统, 4月, 2., 设备, 参加, 环境, 能力, 电话, 注册, 资金, 质量, 落实, 重点, 市场, 投资, 农业, 原件, 电子, 教育, 金融, 开标, 经营, 需求, 专业, 研究, 保障, 法定, 国际, 7月, 30分, 支持, 提升, 全国, 网上, 行业, cn, 营业执照, 条件, 授权, 符合, 城市, 责任, 获取, 计划, 材料, 制度, 中华人民共和国, gov, 施工, 推动, 规划, 品目, 附件, 领域, 水平, 证书, 文化, 代码, 大学, 记录, 报告, 旅游, www, 报价, 8月, 法人, 编号, 复印件, 进一, 副本, 接受, 基金, 体系, 设计, 违法, 综合, 基础, 资质, 聊城市, 中共, 标准, 战略, 6月, 银行, 提高, 加快, 建立, 精神, 记者, 品牌, 预算, 标段, 机制, 购买, 新区, 委员, 学习, 监督, 独立, 证明, 递交, 2016年, 小学, 备, 公章, 领导, 生活, 增长, 支部, 公开, 海洋, 日照市, 学校, 上海, 现场, 职工, 知识, 下载, 磋商, 合格, 本, 保护,...
    */
    docTermFreqs.cache()

    //    val docIds = docTermFreqs.rdd.map(_.getLong(0).toString).zipWithUniqueId().map(_.swap).collect().toMap
    /*
docIds: scala.collection.immutable.Map[Long,String] = Map(645 -> 69172, 69 -> 15420, 1665 -> 182018, 1036 -> 114732, 2822 -> 293709, 3944 -> 410312, 1411 -> 165308, 629 -> 74171, 365 -> 36337, 138 -> 16526, 1190 -> 134640, 2295 -> 252348, 101 -> 7938, 5593 -> 562870, 479 -> 40089, 1105 -> 124576, 347 -> 43272, 5950 -> 609178, 3434 -> 357850, 333 -> 33330, 628 -> 66860, 2499 -> 268719, 234 -> 19733, 0 -> 3315, 3927 -> 407898, 7072 -> 723673, 666 -> 56715, 88 -> 3216, 2363 -> 256190, 408 -> 59755, 170 -> 38522, 582 -> 44374, 2210 -> 245378, 115 -> 14667, 5168 -> 526252, 683 -> 59044, 730 -> 77179, 217 -> 18832, 276 -> 29159, 6868 -> 710124, 3230 -> 338878, 5644 -> 566644, 3417 -> 357765, 308 -> 34801, 3910 -> 405042, 5 -> 1161, 449 -> 54407, 120 -> 16508, 6426 -> 658648, 2380 -> 257567, 6...       */

    val idf = new IDF().setInputCol("features").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs) //.select("id", "tfidfVec")
    //    docTermMatrix.printSchema()
    /*
    root
 |-- id: long (nullable = false)
 |-- urlID: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- segWords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- features: vector (nullable = true)
 |-- tfidfVec: vector (nullable = true)
     */
    /*
+----+--------------------+
|  id|            tfidfVec|
+----+--------------------+
| 595|(8663,[1,2,3,5,6,...|
|1071|(8663,[2,4,5,8,9,...|
|1411|(8663,[1,4,9,10,1...|
|2380|(8663,[3,5,8,10,1...|
|4080|(8663,[48,67,80,1...|
+----+--------------------+
only showing top 5 rows

     */
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
    /*
VS: breeze.linalg.DenseMatrix[Double] =
0.48942423276099606  -7.678959850462754   -35.52087694737469   ... (10 total)
0.7815967342884915   -7.914218155649067   -37.052864078740384  ...
2.7809242423186316   -17.714517591215632  -131.9465556994891   ...
1.889318597963957    -2.1987653429198795  -13.304310120922633  ...
1.162032308403486    -5.693528823395797   -24.41859830625491   ...
0.5493892202405084   -7.588365192469285   -63.54888906284153   ...
1.8106179420774777   -22.37550674572779   -76.79428807120864   ...
1.9227767540063303   -22.73163306235219   -51.18507715508875   ...
0.6237448432406094   -6.663925625916391   -33.294210471691095  ...
0.6088427945347638   -9.13544054198646    -32.23368462072267   ...
1.4769450606924126   -23.98283032282095   -52.955460094803826  ...
5.9089473...
     */
    val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
    /*
normalizedVS: breeze.linalg.DenseMatrix[Double] =
0.007190592486601335   -0.11281883345693455   ... (10 total)
0.005557903155134412   -0.056277689156091534  ...
0.018726091626300272   -0.1192854068013852    ...
0.05175443821215388    -0.06023116757851507   ...
0.01592625010571956    -0.0780327391672139    ...
0.0075681173109323745  -0.10453360906801605   ...
0.018761446513000567   -0.23185281845245953   ...
0.031099108030328546   -0.3676628141248849    ...
0.005182306401426213   -0.05536639669899927   ...
0.004749959556513966   -0.0712712271458048    ...
0.018728144518808068   -0.3041101014585291    ...
0.10720606172559094    -0.0834313687856967    ...
0.007041340763534375   -0.1133332104896027    ...
0.00812934454955762    -0.09476535659875575   ...
0.005527698150737878   -0.8528312005...
     */
    val US: IndexedRowMatrix = multiplyByDiagonalIndexedRowMatrix(svd.U, svd.s)
    val normalizedUS: IndexedRowMatrix = distributedIndexedRowNormalized(US)

    val docVec = normalizedUS.rows.map { vec => {
      val index = vec.index
      val docRowArr = vec.vector.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      // Compute scores against every doc
      //      val docScores = normalizedUS.multiply(docRowVec).rows
      // Find the docs with the highest scores
      //      val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId//.map(_.vector)
      //      val simiIndex = docScores.rows.map(_.index)

      (index, docRowVec)
    }
    }
    val indexList = docVec.map(_._1).collect().toList
    /*
    indexList: List[Long] = List(3315, 6732, 12784, 12937, 23273, 24208, 25585, 29376, 36159, 37179, 38522, 39338, 40664, 41497, 47226, 47243, 47549, 48705, 49147, 49878, 54876, 55692, 56508, 58123, 59755, 60571, 61523, 61710, 63784, 68850, 69462, 69938, 70567, 71587, 72845, 73406, 73474, 74171, 74528, 76143, 79866, 84575, 86003, 86309, 87431, 88655, 90610, 91392, 97750, 97920, 98668, 100300, 100895, 106675, 108239, 110109, 110653, 112591, 113254, 113917, 115583, 116178, 121805, 123148, 123250, 124576, 125851, 127483, 131376, 131512, 134640, 136408, 139264, 144313, 150654, 151334, 153221, 154853, 155363, 158644, 161840, 163438, 164866, 165308, 166736, 167212, 168793, 169167, 170323, 171836, 172618, 175168, 175474, 176630, 176953, 178194, 180200, 183362, 183736, 184484, 184739, 185113, 18524...
     */


    // result4 is the best one! for now....
    val docsimi = new ArrayBuffer[String]()

    indexList.foreach(x => {
      val docRowArr = docVec.lookup(x).head.toArray
      val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)
      val docScores = normalizedUS.multiply(docRowVec).rows
      val id = docScores.map(_.index)
      val score = docScores.map(_.vector.apply(0).toDouble)
      val zipD = id.zip(score).top(10).foreach(y => {
        docsimi ++= Array(x + ";" + y._1 + ";" + y._2)
      }
      )
    })
    //    println("docsimi is: ")
    //    println(docsimi)
    /*

     */
    val docsimiRDD = sc.parallelize(docsimi).map(_.split(";")).map(x => {
      val id = x(0).toLong
      val simiId = x(1).toLong
      val sims = x(2).toDouble
      val simiScore = f"$sims%1.5f".toDouble
      DocsimiIdSchema(id, simiId, simiScore)
    }).filter(_.value >= 0.99)

    //    docsimiRDD.take(10).foreach(println)

    val docsimiDS = spark.createDataset(docsimiRDD)
    val ds1 = docsimiDS.join(doc1IdLab, Seq("doc1Id"), "left")
    val ds2 = ds1.join(doc2IdLab, Seq("doc2Id"), "left")
    //    ds2.printSchema()
    /*
 |-- doc2Id: long (nullable = false)
 |-- doc1Id: long (nullable = false)
 |-- value: double (nullable = false)
 |-- doc1: string (nullable = true)
 |-- doc2: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- time: string (nullable = true)
     */

    //对dataframe进行分组排序，并取每组的前5个
    //计算两个向量的余弦相似度，值越大就表示越相似。
    val w = Window.partitionBy("doc1Id").orderBy(col("value").desc)
    val ds3 = ds2.withColumn("rn", row_number.over(w)).where(col("rn") <= 5)

    //    ds3.printSchema
    /*
root
 |-- doc2Id: long (nullable = false)
 |-- doc1Id: long (nullable = false)
 |-- value: double (nullable = false)
 |-- doc1: string (nullable = true)
 |-- doc2: string (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- websitename: string (nullable = true)
 |-- time: string (nullable = true)
 |-- rn: integer (nullable = true)
     */

    val ds4 = ds3.select("doc1", "doc2", "value", "rn", "title", "label", "time", "websitename")

    /*
     ds4.printSchema
root
 |-- doc1: string (nullable = true)
 |-- doc2: string (nullable = true)
 |-- value: double (nullable = false)
 |-- rn: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- label: string (nullable = true)
 |-- time: string (nullable = true)
 |-- websitename: string (nullable = true)

     */

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
   * Returns a distributed matrix where each row is divided by its length.
   */
  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }


}
