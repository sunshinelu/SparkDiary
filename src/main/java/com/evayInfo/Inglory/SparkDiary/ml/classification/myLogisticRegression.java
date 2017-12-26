package com.evayInfo.Inglory.SparkDiary.ml.classification;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 *  Spark2.0机器学习系列之2：Logistic回归及Binary分类（二分问题）结果评估
 * http://blog.csdn.net/qq_34531825/article/details/52313553
 *
 */


public class myLogisticRegression {

    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("LR")
                .master("local[4]")
                .config("spark.sql.warehouse.dir","file///:G:/Projects/Java/Spark/spark-warehouse" )
                .getOrCreate();
        String path="G:/Projects/CgyWin64/home/pengjy3/softwate/spark-2.0.0-bin-hadoop2.6/"
                + "data/mllib/sample_libsvm_data.txt";

        //屏蔽日志
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);

        //Load trainning data
        Dataset<Row> trainning_dataFrame=spark.read().format("libsvm").load(path);


        LogisticRegression lr=new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.2)
                .setThreshold(0.5);

        //fit the model
        LogisticRegressionModel lrModel=lr.fit(trainning_dataFrame);

        //print the coefficients and intercept for logistic regression
        System.out.println
                ("Coefficient:"+lrModel.coefficients()+"Itercept"+lrModel.intercept());

        //Extract the summary from the returned LogisticRegressionModel
        LogisticRegressionTrainingSummary summary=lrModel.summary();

        //Obtain the loss per iteration.
        double[] objectiveHistory=summary.objectiveHistory();
        for(double lossPerIteration:objectiveHistory){
            System.out.println(lossPerIteration);
        }
        // Obtain the metrics useful to judge performance on test data.
        // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
        // classification problem.
        BinaryLogisticRegressionTrainingSummary binarySummary=
                (BinaryLogisticRegressionTrainingSummary)summary;
        //Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
        Dataset<Row> roc=binarySummary.roc();
        roc.show((int) roc.count());//显示全部的信息，roc.show()默认只显示20行
        roc.select("FPR").show();
        System.out.println(binarySummary.areaUnderROC());

        // Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
        // this selected threshold.
        Dataset<Row> fMeasure = binarySummary.fMeasureByThreshold();
        double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
        double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
                .select("threshold").head().getDouble(0);
        lrModel.setThreshold(bestThreshold);



    }

    /*
    //自定义计算accuracy，
        Dataset<Row> predictDF=naiveBayesModel.transform(test);

        double total=(double) predictDF.count();
        Encoder<Double> doubleEncoder=Encoders.DOUBLE();
        Dataset<Double> accuracyDF=predictDF.map(new MapFunction<Row,Double>() {
            @Override
            public Double call(Row row) throws Exception {
                if((double)row.get(0)==(double)row.get(6)){return 1.0;}
                else {return 0.0;}
            }
        }, doubleEncoder);
        accuracyDF.createOrReplaceTempView("view");
        double correct=(double) spark.sql("SELECT value FROM view WHERE value=1.0").count();
        System.out.println("accuracy "+(correct/total));
     */
}
