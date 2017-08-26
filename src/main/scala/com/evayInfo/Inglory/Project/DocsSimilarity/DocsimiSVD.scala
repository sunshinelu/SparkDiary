package com.evayInfo.Inglory.Project.DocsSimilarity

import breeze.linalg.{DenseMatrix => BDenseMatrix, SparseVector => BSparseVector}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors, Vector => MLLibVector}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


/**
  * Created by sunlu on 17/8/26.
  */

object DocsimiSVD {
  def main(args: Array[String]): Unit = {

    DocsimiUtil.SetLogger

    val sparkConf = new SparkConf().setAppName(s"DocsimiSVD").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    /*
    val ylzxTable = args(0)
    val docSimiTable = args(1)
    */

    val ylzxTable =  "yilan-total_webpage"
    val docSimiTable = "docsimi_svd"

    val ylzxRDD = DocsimiCountVectorizer.getYlzxRDD(ylzxTable, sc)
    val ylzxDS = spark.createDataset(ylzxRDD).randomSplit(Array(0.01, 0.99))(0)

    println("ylzxDS的数量为：" + ylzxDS.count())

    val vocabSize: Int = 20000

    val vocabModel: CountVectorizerModel = new CountVectorizer().
      setInputCol("segWords").
      setOutputCol("features").
      setVocabSize(vocabSize).
      setMinDF(2).
      fit(ylzxDS)

    val docTermFreqs = vocabModel.transform(ylzxDS)
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
    val termIds = vocabModel.vocabulary
    /*
    termIds: Array[String] = Array(nbsp, 采购, 项目, 2017年, 发展, 时间, 服务, xfffd, 工作, 建设, 管理, 文件, 工程, 方式, 招标, 机构, 创新, 资源, 单位, 供应商, 经济, 有限公司, 改革, 地点, 名称, 国家, 信息, 提供, 报名, 资格, 数据, 部门, 山东, 投标, 社会, 相关, 情况, 组织, 北京, 发布, 公共, 联系人, 公司, 生产, 活动, 交易, 人员, 系统, 科技, 地址, 代理, 推进, 市场, 4月, 山东省, 行业, 城市, 济南市, 环境, 重点, 需求, 质量, 内容, 能力, 1., 旅游, 监督, 5月, 公告, 政策, 落实, 专业, 人才, 电话, 参加, 推动, 注册, 2017, 2., 支持, 资金, 设备, 国际, 合作, 实施, 经营, 责任, 开标, 政务, 投资, 教育, 体系, 投标人, 30, 规划, 学习, 建立, 00, 会议, 营业执照, 创业, 研究, 原件, 公开, 提高, 制度, 7月, 资, 法定, 领域, 增长, 标准, 条件, 施工, 济南, 提升, 获取, 品目, 电子, 授权, 意见, 提出, 健康, 材料, 金融, 机制, 记录, 磋商, 计划, 附件, 预算, 保障, 符合, 保护, 农业, 中华人民共和国, 6月, 代码, 复印件, 接受, 全国, 编号, 30分, 2016年, 合格, 综合, 证书, 行政, 信用, 文化, 生态, 担保, 报价, 医疗, 网上, 8月, 网站, 违法, 基础, 工业, 建筑, 业务, 设计, 领导, 资质, 方案, 进一步, 加快, 咨询, 完善, 监管, 法律, 办理, 报告, cn, 具备, 独立, 改造, 副本, 交通, 日照市, 证明, 作用, 水平,...
     */
    docTermFreqs.cache()

    val docIds = docTermFreqs.rdd.map(_.getLong(0).toString).zipWithUniqueId().map(_.swap).collect().toMap
      /*
      docIds: scala.collection.immutable.Map[Long,String] = Map(645 -> 69342, 69 -> 4574, 1665 -> 167262, 1036 -> 107728, 2822 -> 297670, 3944 -> 418744, 1411 -> 154530, 629 -> 70295, 365 -> 32665, 138 -> 20419, 1190 -> 129982, 2295 -> 253878, 101 -> 8822, 5593 -> 587265, 1767 -> 178006, 479 -> 44679, 1105 -> 118354, 347 -> 46468, 5950 -> 634236, 3434 -> 373388, 628 -> 68781, 2499 -> 266186, 234 -> 24102, 0 -> 595, 3927 -> 418540, 666 -> 58551, 1818 -> 178618, 88 -> 7279, 2363 -> 256190, 1750 -> 175269, 408 -> 50286, 170 -> 20417, 582 -> 75093, 2210 -> 246483, 115 -> 15534, 5168 -> 552619, 683 -> 58619, 730 -> 83775, 217 -> 22776, 276 -> 32831, 6868 -> 726002, 3230 -> 355691, 5644 -> 593963, 3417 -> 372912, 308 -> 31027, 3910 -> 417452, 5 -> 2164, 449 -> 60085, 120 -> 6070, 6426 -> 687786, 51...
       */

    val idf = new IDF().setInputCol("features").setOutputCol("tfidfVec")
    val idfModel = idf.fit(docTermFreqs)
    val docTermMatrix = idfModel.transform(docTermFreqs).select("id", "tfidfVec")
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
    val vecRdd = docTermMatrix.select("tfidfVec").rdd.map { row =>
      Vectors.fromML(row.getAs[MLVector]("tfidfVec"))
    }

    vecRdd.cache()
    val mat = new RowMatrix(vecRdd)
    val k = 100
    val svd = mat.computeSVD(k, computeU=true)

    val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
    /*
    VS: breeze.linalg.DenseMatrix[Double] =
-0.0047136965321909275  1889.5941688390822     ... (100 total)
-4.581781976509973E-4   69.46147227368573      ...
-0.007627676361205102   32.47584001035988      ...
-1.9686446876906758E-4  22.587331041530184     ...
-0.05847391106498378    4.354826587420148      ...
-0.0023326910495038355  35.97885440028559      ...
-0.017338899453212413   17.507465085941824     ...
-7749.886547448817      -0.020404629609707133  ...
-0.035405374419757385   6.668060005015721      ...
-0.007159115442379753   14.32442481372888      ...
-0.017012865045531362   16.928771129897363     ...
-4.36796318420922E-4    47.583465410163726     ...
-0.008427700483434752   26.569485311718257     ...
-9.714176148714257E-4   17.20843727171209      ...
-3.4197132283486273E-4  45.6091...
     */
    val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
    /*
    normalizedVS: breeze.linalg.DenseMatrix[Double] =
-2.4931671813546273E-6  0.9994436713640039     ... (100 total)
-3.3219135576977953E-6  0.503614112732987      ...
-1.144639886677004E-4   0.4873455567446996     ...
-4.066690742005669E-6   0.4665935433019132     ...
-2.9138116059960556E-4  0.02170052254316152    ...
-2.917988022603773E-5   0.4500633130536894     ...
-1.6242759917869964E-4  0.16400669081033165    ...
-0.9999999999424221     -2.632893976396244E-6  ...
-2.5814428453224843E-4  0.048617522266685274   ...
-5.932072088733295E-5   0.11869276492129559    ...
-1.0486208912893492E-4  0.10434375999079074    ...
-3.54398551360484E-6    0.38607264985743484    ...
-3.993856697252377E-5   0.12591182738794607    ...
-2.8183978966348773E-5  0.49927263690175044    ...
-2.8125188846336485E-...
     */
    val US: RowMatrix = multiplyByDiagonalRowMatrix(svd.U, svd.s)
    val normalizedUS: RowMatrix = distributedRowsNormalized(US)

    /**
      * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
      * relevance scores to the given doc.
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

    topDocsForDoc(1)
    /*
       res13: Seq[(Double, Long)] = WrappedArray((1.0000000000000002,1), (0.8429471053253086,103), (0.8096031279923385,564), (0.8058547415021463,69), (0.8015145343856134,223), (0.8012451533741273,258), (0.7756941070911659,785), (0.7743618146673477,75), (0.7661314715075367,173), (0.7626197230297377,86))

     */


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
