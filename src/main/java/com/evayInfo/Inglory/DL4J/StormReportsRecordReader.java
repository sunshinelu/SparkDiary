package com.evayInfo.Inglory.DL4J;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.transform.TransformProcess;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.SparkTransformExecutor;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.datavec.spark.transform.misc.WritablesToStringFunction;

import java.util.Date;
import java.util.List;

/**
 * Created by tomhanlon on 10/17/16.
 * 代码链接：
 * https://www.youtube.com/watch?v=8EIBIfVlgmU&t=3s
 * https://github.com/SkymindIO/screencasts
 */
public class StormReportsRecordReader {
    public static void main(String[] args)throws Exception {
        int numLinesToSkip = 0;
        String delimiter = ",";

        /**
         * Specify the root directory
         * If you are working from home replace baseDir
         * with the location you downloaded the reports.csv
         * file to.
         */

        String baseDir = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/DL4J/";
        String fileName = "reports.csv";
        String inputPath = baseDir + fileName;
        String timeStamp = String.valueOf(new Date().getTime());
        String outputPath = baseDir + "reports_processed_" + timeStamp;

        /**
         * Data file looks like this
         * 161006-1655,UNK,2 SE BARTLETT,LABETTE,KS,37.03,-95.19,
         * TRAINED SPOTTER REPORTS TORNADO ON THE GROUND. (ICT),TOR
         * Fields are
         * datetime,severity,location,county,state,lat,lon,comment,type
         */

        Schema inputDataSchema = new Schema.Builder()
                .addColumnsString("datetime","severity","location","county","state")
                .addColumnsDouble("lat","lon")
                .addColumnsString("comment")
                .addColumnCategorical("type","TOR","WIND","HAIL")
                .build();


        /**
         * Define a transform process to extract lat and lon
         * and also transform type from one of three strings
         * to either 0,1,2
         */


        TransformProcess tp = new TransformProcess.Builder(inputDataSchema)
                .removeColumns("datetime","severity","location","county","state","comment")
                .categoricalToInteger("type")
                .build();
/**
 * Some code to step through and print the before
 * and after Schema
 */

        int numActions = tp.getActionList().size();
        for (int i = 0; i<numActions; i++){
            System.out.println("\n\n===============================");
            System.out.println("--- Schema after step " + i +
            " (" + tp.getActionList().get(i) + ")--" );
            System.out.println(tp.getSchemaAfterStep(i));
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[*]");
        sparkConf.setAppName("Storm Reports Record Reader Transform");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        /**
         * Get our data into a spark RDD
         * and transform that spark RDD using our
         * transform process
         */

        // read the data file
        JavaRDD<String> lines = sc.textFile(inputPath);
        // convert to Writable
        JavaRDD<List<Writable>> stormReports = lines.map(new StringToWritablesFunction(new CSVRecordReader()));
        // run our transform process
        JavaRDD<List<Writable>> processed = SparkTransformExecutor.execute(stormReports,tp);
        // convert Writable back to string for export
        JavaRDD<String> toSave= processed.map(new WritablesToStringFunction(","));

        toSave.saveAsTextFile(outputPath);


    }

}
