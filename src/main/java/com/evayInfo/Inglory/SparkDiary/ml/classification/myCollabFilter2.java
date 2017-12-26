package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 *  Spark2.0 协同过滤推荐
 * http://blog.csdn.net/qq_34531825/article/details/52319449
 *
 */
public class myCollabFilter2 {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("CoFilter")
                .master("local[4]")
                .config("spark.sql.warehouse.dir","file///:G:/Projects/Java/Spark/spark-warehouse" )
                .getOrCreate();

        String path="G:/Projects/CgyWin64/home/pengjy3/softwate/spark-2.0.0-bin-hadoop2.6/"
                + "data/mllib/als/sample_movielens_ratings.txt";

        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        //-------------------------------1.0 准备DataFrame----------------------------
        //..javaRDD()函数将DataFrame转换为RDD
        //然后对RDD进行Map 每一行String->Rating
        JavaRDD<Rating> ratingRDD=spark.read().textFile(path).javaRDD()
                .map(new Function<String, Rating>() {

                    @Override
                    public Rating call(String str) throws Exception {
                        return Rating.parseRating(str);
                    }
                });
        //System.out.println(ratingRDD.take(10).get(0).getMovieId());

        //由JavaRDD(每一行都是一个实例化的Rating对象)和Rating Class创建DataFrame
        Dataset<Row> ratings=spark.createDataFrame(ratingRDD, Rating.class);
        //ratings.show(30);

        //将数据随机分为训练集和测试集
        double[] weights=new double[] {0.8,0.2};
        long seed=1234;
        Dataset<Row> [] split=ratings.randomSplit(weights, seed);
        Dataset<Row> training=split[0];
        Dataset<Row> test=split[1];

        //------------------------------2.0 ALS算法和训练数据集，产生推荐模型-------------
        for(int rank=1;rank<20;rank++)
        {
            //定义算法
            ALS als=new ALS()
                    .setMaxIter(5)////最大迭代次数,设置太大发生java.lang.StackOverflowError
                    .setRegParam(0.16)
                    .setUserCol("userId")
                    .setRank(rank)
                    .setItemCol("movieId")
                    .setRatingCol("rating");
            //训练模型
            ALSModel model=als.fit(training);
            //---------------------------3.0 模型评估：计算RMSE，均方根误差---------------------
            Dataset<Row> predictions=model.transform(test);
            //predictions.show();
            RegressionEvaluator evaluator=new RegressionEvaluator()
                    .setMetricName("rmse")
                    .setLabelCol("rating")
                    .setPredictionCol("prediction");
            Double rmse=evaluator.evaluate(predictions);
            System.out.println("Rank =" + rank+"  RMSErr = " + rmse);
        }
    }
    /*
     //循环正则化参数，每次由Evaluator给出RMSError
      List<Double> RMSE=new ArrayList<Double>();//构建一个List保存所有的RMSE
      for(int i=0;i<20;i++){//进行20次循环
          double lambda=(i*5+1)*0.01;//RegParam按照0.05增加
          ALS als = new ALS()
          .setMaxIter(5)//最大迭代次数
          .setRegParam(lambda)//正则化参数
          .setUserCol("userId")
          .setItemCol("movieId")
          .setRatingCol("rating");
          ALSModel model = als.fit(training);
          // Evaluate the model by computing the RMSE on the test data
          Dataset<Row> predictions = model.transform(test);
          //RegressionEvaluator.setMetricName可以定义四种评估器
          //"rmse" (default): root mean squared error
          //"mse": mean squared error
          //"r2": R^2^ metric
          //"mae": mean absolute error
          RegressionEvaluator evaluator = new RegressionEvaluator()
          .setMetricName("rmse")//RMS Error
          .setLabelCol("rating")
          .setPredictionCol("prediction");
          Double rmse = evaluator.evaluate(predictions);
          RMSE.add(rmse);
          System.out.println("RegParam "+0.01*i+" RMSE " + rmse+"\n");
      }
      //输出所有结果
      for (int j = 0; j < RMSE.size(); j++) {
          Double lambda=(j*5+1)*0.01;
          System.out.println("RegParam= "+lambda+"  RMSE= " + RMSE.get(j)+"\n");
    }

    //可以用SPARK-SQL自己定义评估算法（如下面定义了一个平均绝对值误差计算过程）
                    // Register the DataFrame as a SQL temporary view
                    predictions.createOrReplaceTempView("tmp_predictions");
                    Dataset<Row> absDiff=spark.sql("select abs(prediction-rating) as diff from tmp_predictions");
                    absDiff.createOrReplaceTempView("tmp_absDiff");
                    spark.sql("select mean(diff) as absMeanDiff from tmp_absDiff").show();

     */
}
