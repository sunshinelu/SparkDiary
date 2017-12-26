package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassifier;
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
 * Spark2.0机器学习系列之5：GBDT（梯度提升决策树）、GBDT与随机森林差异、参数调试及Scikit代码分析
 * http://blog.csdn.net/qq_34531825/article/details/52366265
 */

public class myGDBT {
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
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);//WARN
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        Dataset<Row> training=spark.read().format("libsvm").load(path);
        Dataset<Row> test=spark.read().format("libsvm").load(path2);

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
        //调试参数MaxIter,learningRate,maxDepth，也对两种不纯度进行了测试
        for (int MaxIter = 30; MaxIter < 40; MaxIter+=10)
            for (int maxDepth = 2; maxDepth < 3; maxDepth+=1)
                for (int impurityType = 1; impurityType <2; impurityType+=1)
                    for (int setpSize = 1; setpSize< 10; setpSize+=1) {
                        long begin = System.currentTimeMillis();//训练开始时间
                        String impurityType_=null;//不纯度类型选择
                        if (impurityType==1) {
                            impurityType_="gini";
                        }
                        else  {
                            impurityType_="entropy";
                        }
                        double setpSize_=0.1*setpSize;
                        GBTClassifier gbtClassifier=new GBTClassifier()
                                .setLabelCol("indexedLabel")
                                .setFeaturesCol("indexedFeatures")
                                .setMaxIter(MaxIter)
                                .setImpurity(impurityType_)//.setImpurity("entropy")
                                .setMaxDepth(maxDepth)
                                .setStepSize(setpSize_)//范围是(0, 1]
                                .setSeed(1234);

                        PipelineModel pipeline=new Pipeline().setStages
                                (new PipelineStage[]
                                        {indexerModel,vectorIndexerModel,gbtClassifier,converter})
                                .fit(training);
                        long end=System.currentTimeMillis();

                        //一定要在测试数据集上做验证
                        Dataset<Row> predictDataFrame=pipeline.transform(test);

                        double accuracy=new MulticlassClassificationEvaluator()
                                .setLabelCol("indexedLabel")
                                .setPredictionCol("prediction")
                                .setMetricName("accuracy").evaluate(predictDataFrame);
                        String str_accuracy=String.format(" accuracy = %.4f ", accuracy);
                        String str_time=String.format(" trainig time = %d ", (end-begin));
                        String str_maxIter=String.format(" maxIter = %d ", MaxIter);
                        String str_maxDepth=String.format(" maxDepth = %d ", maxDepth);
                        String str_stepSize=String.format(" setpSize = %.2f ", setpSize_);
                        String str_impurityType_=" impurityType = "+impurityType_;
                        System.out.println(str_maxIter+str_maxDepth+str_impurityType_+
                                str_stepSize+str_accuracy+str_time);

                    }//Params Cycle
    }
}
