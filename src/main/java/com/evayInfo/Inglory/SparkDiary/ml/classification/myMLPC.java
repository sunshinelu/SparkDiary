package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 *  Spark2.0机器学习系列之6： MLPC（多层神经网络）
 *  http://blog.csdn.net/qq_34531825/article/details/52381625
 *
 */
public class myMLPC {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("MLPC")
                .master("local[4]")
//                .config("spark.sql.warehouse.dir","file///:G:/Projects/Java/Spark/spark-warehouse" )
                .getOrCreate();
//        String path="G:/Projects/CgyWin64/home/pengjy3/softwate/spark-2.0.0-bin-hadoop2.6/"
//                + "data/mllib/sample_multiclass_classification_data.txt";
        String path= "file:///Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sample_multiclass_classification_data.txt";

        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        //加载数据,randomSplit时加了一个固定的种子seed=100，
        //是为了得到可重复的结果，方便调试算法，实际工作中不能这样设置
        Dataset<Row>[] split=spark.read().format("libsvm").load(path).randomSplit(new double[]{0.6,0.4},100);
        Dataset<Row> training=split[0];
        Dataset<Row> test=split[1];
        training.show(100,false);//数据检查

        //第一层树特征个数
        //最后一层，即输出层是labels个数（类数）
        //隐藏层自己定义
        int[] layer=new int[]{4,6,4,3};

        int[] maxIter=new int[]{5,10,20,50,100,200};
        double[] accuracy=new double[]{0,0,0,0,0,0,0,0,0,0};
        //利用如下类似的循环可以很方便的对各种参数进行调优
        for(int i=0;i<maxIter.length;i++){
            MultilayerPerceptronClassifier multilayerPerceptronClassifier=
                    new MultilayerPerceptronClassifier()
                            .setLabelCol("label")
                            .setFeaturesCol("features")
                            .setLayers(layer)
                            .setMaxIter(maxIter[i])
                            .setBlockSize(128)
                            .setSeed(1000);
            MultilayerPerceptronClassificationModel model=
                    multilayerPerceptronClassifier.fit(training);

            Dataset<Row> predictions=model.transform(test);
            MulticlassClassificationEvaluator evaluator=
                    new MulticlassClassificationEvaluator()
                            .setLabelCol("label")
                            .setPredictionCol("prediction")
                            .setMetricName("accuracy");
            accuracy[i]=evaluator.evaluate(predictions);
        }

        //一次性输出所有评估结果
        for(int j=0;j<maxIter.length;j++){
            String str_accuracy=String.format(" accuracy =  %.2f", accuracy[j]);
            String str_maxIter=String.format(" maxIter =  %d", maxIter[j]);
            System.out.println(str_maxIter+str_accuracy);
        }
    }

}
