package com.evayInfo.Inglory.NLP.Ansj

import org.ansj.app.keyword.KeyWordComputer
import org.ansj.recognition.NatureRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/10/12.
 */
object AnsjDemo3 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  def main(args: Array[String]) {
    /*
    val strs: Array[String] = Array("对", "非", "ansj", "的", "分词", "结果", "进行", "词性", "标注")
    val lists: List[String] = Arrays.asList(strs)
    val recognition: List[Term] = NatureRecognition.recognition(lists, 0)
    System.out.println(recognition)
    */
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"AnsjDemo3").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val title = "维基解密否认斯诺登接受委内瑞拉庇护"
    val content = "有俄罗斯国会议员，9号在社交网站推特表示，美国中情局前雇员斯诺登，已经接受委内瑞拉的庇护，不过推文在发布几分钟后随即删除。俄罗斯当局拒绝发表评论，而一直协助斯诺登的维基解密否认他将投靠委内瑞拉。　　俄罗斯国会国际事务委员会主席普什科夫，在个人推特率先披露斯诺登已接受委内瑞拉的庇护建议，令外界以为斯诺登的动向终于有新进展。　　不过推文在几分钟内旋即被删除，普什科夫澄清他是看到俄罗斯国营电视台的新闻才这样说，而电视台已经作出否认，称普什科夫是误解了新闻内容。　　委内瑞拉驻莫斯科大使馆、俄罗斯总统府发言人、以及外交部都拒绝发表评论。而维基解密就否认斯诺登已正式接受委内瑞拉的庇护，说会在适当时间公布有关决定。　　斯诺登相信目前还在莫斯科谢列梅捷沃机场，已滞留两个多星期。他早前向约20个国家提交庇护申请，委内瑞拉、尼加拉瓜和玻利维亚，先后表示答应，不过斯诺登还没作出决定。　　而另一场外交风波，玻利维亚总统莫拉莱斯的专机上星期被欧洲多国以怀疑斯诺登在机上为由拒绝过境事件，涉事国家之一的西班牙突然转口风，外长马加略]号表示愿意就任何误解致歉，但强调当时当局没有关闭领空或不许专机降落。";

    val df = sc.parallelize(Seq(("a", title, content), ("b", title, content))).toDF("id", "title", "content")
    df.show()

    //定义UDF
    //关键词提取
    def getKeyWordsFunc(title: String, content: String): String = {
      //每篇文章提取5个关键词
      val kwc = new KeyWordComputer(5)
      val keywords = kwc.computeArticleTfidf(title, content).toString.replace("[", "").replace("]", "")

      val result = keywords match {
        case r if (r.length >= 2) => r
        case _ => "NULL" // Seq("null")
      }
      result
    }
    val KeyWordsUDF = udf((title: String, content: String) => getKeyWordsFunc(title, content))
    val df1 = df.withColumn("getKW", KeyWordsUDF($"title", $"content")).drop("content").drop("title") //.filter(!$"segWords".contains("null"))
    df1.show(false)

    val df2 = df1.withColumn("KwW", explode(split($"getKW", ",")))
    df2.show(false)

    val df3 = df2.withColumn("words_temp", split($"KwW", "/")(0)).
      withColumn("words", regexp_replace($"words_temp", " ", "")).drop("words_temp").
      withColumn("weight", split($"KwW", "/")(1)).
      drop("getKW").drop("KwW").
      withColumn("weight", col("weight").cast("double"))
    df3.show(false)

    val w = Window.partitionBy($"id").orderBy($"weight".asc)
    val df4 = df3.withColumn("rn", row_number.over(w))
    df4.show(false)

    val df5 = df4.groupBy("words").agg(sum($"rn")).withColumnRenamed("sum(rn)", "v")
    df5.show(false)

    //获取词性
    def getNaturesFunc(content: String): String = {
      val terms = ToAnalysis.parse(content)
      new NatureRecognition(terms).recognition()
      val natures = terms.toString.replace("[", "").replace("]", "").split("/")(1) //.mkString("")
      val result = natures match {
          case r if (!r.contains(",")) => r
          case _ => "n"
        }
      result
    }
    val NaturesUDF = udf((content: String) => getNaturesFunc(content))

    val df6 = df5.withColumn("nature", NaturesUDF($"words"))
    df6.show(false)

    sc.stop()
    spark.stop()
  }
}
