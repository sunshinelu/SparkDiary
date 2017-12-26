package com.evayInfo.Inglory.SparkDiary.ml.classification;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;

/**
 * Created by sunlu on 17/12/26.
 * 参考链接：
 *  Spark2.0机器学习系列之1：基于Pipeline、交叉验证、ParamMap的模型选择和超参数调优
 * http://blog.csdn.net/qq_34531825/article/details/52334436
 * http://spark.apache.org/docs/latest/ml-collaborative-filtering.html
 */

public class Rating implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating() {}

    public Rating(int userId, int movieId, float rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Rating parseRating(String str) {
        String[] fields = str.split("::");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int movieId = Integer.parseInt(fields[1]);
        float rating = Float.parseFloat(fields[2]);
        long timestamp = Long.parseLong(fields[3]);
        return new Rating(userId, movieId, rating, timestamp);
    }
}
