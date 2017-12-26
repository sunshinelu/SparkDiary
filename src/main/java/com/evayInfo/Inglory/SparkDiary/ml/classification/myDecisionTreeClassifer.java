package com.evayInfo.Inglory.SparkDiary.ml.classification;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
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
 * Spark2.0机器学习系列之3：决策树及Spark 2.0-MLlib、Scikit代码分析
 * http://blog.csdn.net/qq_34531825/article/details/52330942
 *
 */

public class myDecisionTreeClassifer {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("myDecisonTreeClassifer")
                .getOrCreate();
        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        //-------------------0 加载数据------------------------------------------
        String path = "/home/hadoop/spark/spark-2.0.0-bin-hadoop2.6" +
                "/data/mllib/sample_multiclass_classification_data.txt";
        //"/data/mllib/sample_libsvm_data.txt";
        Dataset<Row> rawData = spark.read().format("libsvm").load(path);
        Dataset<Row>[] split = rawData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = split[0];
        Dataset<Row> test = split[1];

        //rawData.show(100);//加载数据检查，显示100行数，每一行都不截断
        //-------------------1 建立决策树训练的Pipeline-------------------------------
        //1.1 对label进行重新编号
        StringIndexerModel labelIndexerModel = new StringIndexer().
                setInputCol("label")
                .setOutputCol("indexedLabel")
                //.setHandleInvalid("error")
                .setHandleInvalid("skip")
                .fit(rawData);
        //1.2 对特征向量进行重新编号
        // Automatically identify categorical features, and index them.
        // Set maxCategories so features with > 5 distinct values are
        //treated as continuous.
        //针对离散型特征而言的，对离散型特征值进行编号。
        //.setMaxCategories(5)表示假如特征值的取值多于四种，则视为连续值
        //也就是这样设置就无效了
        VectorIndexerModel featureIndexerModel = new VectorIndexer()
                .setInputCol("features")
                .setMaxCategories(5)
                .setOutputCol("indexedFeatures")
                .fit(rawData);
        //1.3 决策树分类器
    /*DecisionTreeClassifier dtClassifier=
            new DecisionTreeClassifier()
            .setLabelCol("indexedLabel")//使用index后的label
            .setFeaturesCol("indexedFeatures");//使用index后的features
            */
        //1.3 决策树分类器参数设置
        for (int maxDepth = 2; maxDepth < 10; maxDepth++) {
            DecisionTreeClassifier dtClassifier = new DecisionTreeClassifier()
                    .setLabelCol("indexedLabel")
                    .setFeaturesCol("indexedFeatures")
                    .setMaxDepth(maxDepth)
                    //.setMinInfoGain(0.5)
                    //.setMinInstancesPerNode(10)
                    //.setImpurity("gini")//Gini不纯度
                    .setImpurity("entropy")//或者熵
                    //.setMaxBins(100）//其它可调试的还有一些参数
                    ;

            //1.4 将编号后的预测label转换回来
            IndexToString converter = new IndexToString()
                    .setInputCol("prediction")//自动产生的预测label行名字
                    .setOutputCol("convetedPrediction")
                    .setLabels(labelIndexerModel.labels());
            //Pileline这四个阶段，
            Pipeline pipeline = new Pipeline()
                    .setStages(new PipelineStage[]
                            {labelIndexerModel,
                                    featureIndexerModel,
                                    dtClassifier,
                                    converter});
            //在训练集上训练pipeline模型
            PipelineModel pipelineModel = pipeline.fit(training);
            //-----------------------------3 多分类结果评估----------------------------
            //预测
            Dataset<Row> testPrediction = pipelineModel.transform(test);
            MulticlassClassificationEvaluator evaluator =
                    new MulticlassClassificationEvaluator()
                            .setLabelCol("indexedLabel")
                            .setPredictionCol("prediction")
                            .setMetricName("accuracy");
            //评估
            System.out.println("MaxDepth is: " + maxDepth);
            double accuracy = evaluator.evaluate(testPrediction);
            System.out.println("accuracy is: " + accuracy);
            //输出决策树模型
            DecisionTreeClassificationModel treeModel =
                    (DecisionTreeClassificationModel) (pipelineModel.stages()[2]);
            System.out.println("Learned classification tree model depth"
                    + treeModel.depth() + " numNodes " + treeModel.numNodes());
            //+ treeModel.toDebugString());   //输出整个决策树规则集
        }//maxDepth循环
    }
}
