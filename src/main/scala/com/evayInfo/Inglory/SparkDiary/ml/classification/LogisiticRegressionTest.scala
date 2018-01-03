package com.evayInfo.Inglory.SparkDiary.ml.classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
 * Created by sunlu on 18/1/3.
 * https://www.shiyanlou.com/courses/1003/labs/4242/document
 */
object LogisiticRegressionTest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }


  def main(args: Array[String]) {
    SetLogger

    val sparkConf = new SparkConf().setAppName(s"LogisiticRegressionTest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //读取数据，传入数据路径/opt/train/bank_marketing_data.csv
    val bank_Marketing_Data=spark.read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/shiyanlou/bank_marketing_data.csv")
    //查看营销数据的前5条记录，包括所有字段
    println("all columns data:")
    bank_Marketing_Data.show(5)
    //读取营销数据指定的11个字段，并将age、duration、previous三个字段的类型从Integer类型转换为Double类型
    val selected_Data=bank_Marketing_Data.select("age",
      "job",
      "marital",
      "default",
      "housing",
      "loan",
      "duration",
      "previous",
      "poutcome",
      "empvarrate",
      "y")
      .withColumn("age", bank_Marketing_Data("age").cast(DoubleType))
      .withColumn("duration", bank_Marketing_Data("duration").cast(DoubleType))
      .withColumn("previous", bank_Marketing_Data("previous").cast(DoubleType))
    //显示前5条记录，只包含指定的11个字段
    println("11 columns data:")
    selected_Data.show(5)
    //显示营销数据的数据量
    println("data count:"+selected_Data.count())

    //对数据进行概要统计
    val summary = selected_Data.describe()
    println("Summary Statistics:")
    //显示概要统计信息
    summary.show()

    //查看每一列所包含的不同值数量
    val columnNames = selected_Data.columns
    val uniqueValues_PerField = columnNames.map { field => field+":"+selected_Data.select(field).distinct().count() }
    println("Unique Values For each Field:")
    uniqueValues_PerField.foreach(println)


    val indexer = new StringIndexer().setInputCol("job").setOutputCol("jobIndex")
    val indexed = indexer.fit(selected_Data).transform(selected_Data)
    indexed.printSchema()
    indexed.show
    val encoder = new OneHotEncoder().setDropLast(false).setInputCol("jobIndex").setOutputCol("jobVec")
    val encoded = encoder.transform(indexed)
    encoded.show()
    encoded.printSchema()


    val maritalIndexer=new StringIndexer().setInputCol("marital").setOutputCol("maritalIndex")
    //注意：此处所使用的数据是job列应用OneHotEncoder算法后产生的数据encoded
    //这里不能使用原始数据selected_Data，因为原始数据中没有jobVec列。
    val maritalIndexed=maritalIndexer.fit(encoded).transform(encoded)
    val maritalEncoder=new OneHotEncoder().setDropLast(false).setInputCol("maritalIndex").setOutputCol("maritalVec")
    val maritalEncoded=maritalEncoder.transform(maritalIndexed)

    val defaultIndexer=new StringIndexer().setInputCol("default").setOutputCol("defaultIndex")
    //注意：此处所使用的数据是对marital列应用OneHotEncoder算法后产生的数据maritalEncoded
    val defaultIndexed=defaultIndexer.fit(maritalEncoded).transform(maritalEncoded)
    val defaultEncoder=new OneHotEncoder().setDropLast(false).setInputCol("defaultIndex").setOutputCol("defaultVec")
    val defaultEncoded=defaultEncoder.transform(defaultIndexed)

    val housingIndexer=new StringIndexer().setInputCol("housing").setOutputCol("housingIndex")
    //注意：此处所使用的数据是对default列应用OneHotEncoder算法后产生的数据defaultEncoded
    val housingIndexed=housingIndexer.fit(defaultEncoded).transform(defaultEncoded)
    val housingEncoder=new OneHotEncoder().setDropLast(false).setInputCol("housingIndex").setOutputCol("housingVec")
    val housingEncoded=housingEncoder.transform(housingIndexed)

    val poutcomeIndexer=new StringIndexer().setInputCol("poutcome").setOutputCol("poutcomeIndex")
    //注意：此处所使用的数据是对housing列应用OneHotEncoder算法后产生的数据housingEncoded
    val poutcomeIndexed=poutcomeIndexer.fit(housingEncoded).transform(housingEncoded)
    val poutcomeEncoder=new OneHotEncoder().setDropLast(false).setInputCol("poutcomeIndex").setOutputCol("poutcomeVec")
    val poutcomeEncoded=poutcomeEncoder.transform(poutcomeIndexed)

    val loanIndexer=new StringIndexer().setInputCol("loan").setOutputCol("loanIndex")
    //注意：此处所使用的数据是对poutcome列应用OneHotEncoder算法后产生的数据poutcomeEncoded
    val loanIndexed=loanIndexer.fit(poutcomeEncoded).transform(poutcomeEncoded)
    val loanEncoder=new OneHotEncoder().setDropLast(false).setInputCol("loanIndex").setOutputCol("loanVec")
    val loanEncoded=loanEncoder.transform(loanIndexed)
    loanEncoded.show()
    loanEncoded.printSchema()


    //实例化一个向量组装器对象，
    //将向量类型字段("jobVec","maritalVec", "defaultVec","housingVec","poutcomeVec","loanVec")
    //和数值型字段("age","duration","previous","empvarrate")
    //形成一个新的字段:features，其中包含了所有的特征值
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("jobVec","maritalVec", "defaultVec","housingVec","poutcomeVec","loanVec","age","duration","previous","empvarrate"))
      .setOutputCol("features")

    //对目标变量进行StringIndexer特征转换,输出新列:label
    val indexerY = new StringIndexer().setInputCol("y").setOutputCol("label")
    //将特征算法按顺序进行合并，形成一个算法数组
    val transformers=Array(indexer,
      encoder,
      maritalIndexer,
      maritalEncoder,
      defaultIndexer,
      defaultEncoder,
      housingIndexer,
      housingEncoder,
      poutcomeIndexer,
      poutcomeEncoder,
      loanIndexer,
      loanEncoder,
      vectorAssembler,
      indexerY);
    //将原始数据selected_Data进行8-2分，80%用于训练数据。20%用于测试数据，评估训练模型的精确度。
    val splits = selected_Data.randomSplit(Array(0.8,0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    //实例化逻辑回归算法
    val lr = new LogisticRegression()
    //将算法数组和逻辑回归算法合并,传入pipeline对象的stages中，然后作用于训练数据，训练模型
    var model = new Pipeline().setStages(transformers :+ lr).fit(training)

    //将上一步的训练模型作用于测试数据，返回测试结果
    var result = model.transform(test)
    //显示测试结果集中的真实值、预测值、原始值、百分比字段
    result.select("label", "prediction","rawPrediction","probability").show(10,false)
    //创建二分类算法评估器，对测试结果进行评估
    val evaluator = new BinaryClassificationEvaluator()
    var aucTraining = evaluator.evaluate(result)
    println("aucTraining = "+aucTraining)




    sc.stop()
    spark.stop()
  }

}
