package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 *  Spark2.0机器学习系列之4：随机森林介绍、关键参数分析
 * http://blog.csdn.net/qq_34531825/article/details/52352737
 */

public class myRandomForest {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("CoFilter")
                .master("local[4]")
                .config("spark.sql.warehouse.dir",
                        "file///:G:/Projects/Java/Spark/spark-warehouse" )
                .getOrCreate();

        String path="C:/Users/user/Desktop/ml_dataset/classify/horseColicTraining2libsvm.txt";
        String path2="C:/Users/user/Desktop/ml_dataset/classify/horseColicTest2libsvm.txt";
        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        Dataset<Row> training=spark.read().format("libsvm").load(path);
        Dataset<Row> test=spark.read().format("libsvm").load(path2);
        //采用的数据集是《机器学习实战》一书中所用到一个比较难分数据集：
        //从疝气病症预测马的死亡率,加载前用Python将格式转换为
        //libsvm格式（比较简单的一种Spark SQL DataFrame输入格式）
        //这种格式导入的数据，label和features自动分好了，不需要再做任何抓换了。

        StringIndexerModel indexerModel=new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(training);
        VectorIndexerModel vectorIndexerModel=new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .fit(training);
        IndexToString converter=new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("convertedPrediction")
                .setLabels(indexerModel.labels());
        //设计了一个简单的循环，对关键参数（决策树的个数）进行分析调试
        for (int numOfTrees = 10; numOfTrees < 500; numOfTrees+=50) {
            RandomForestClassifier rfclassifer=new RandomForestClassifier()
                    .setLabelCol("indexedLabel")
                    .setFeaturesCol("indexedFeatures")
                    .setNumTrees(numOfTrees);
            PipelineModel pipeline=new Pipeline().setStages
                    (new PipelineStage[]
                            {indexerModel,vectorIndexerModel,rfclassifer,converter})
                    .fit(training);

            Dataset<Row> predictDataFrame=pipeline.transform(test);

            double accuracy=new MulticlassClassificationEvaluator()
                    .setLabelCol("indexedLabel")
                    .setPredictionCol("prediction")
                    .setMetricName("accuracy").evaluate(predictDataFrame);

            System.out.println("numOfTrees "+numOfTrees+" accuracy "+accuracy);
            //RandomForestClassificationModel rfmodel=
            //(RandomForestClassificationModel) pipeline.stages()[2];
            //System.out.println(rfmodel.toDebugString());
        }//numOfTree Cycle
    }
}
