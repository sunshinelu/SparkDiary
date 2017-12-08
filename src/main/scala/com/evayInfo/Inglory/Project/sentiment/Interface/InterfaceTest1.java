package com.evayInfo.Inglory.Project.sentiment.Interface;

/**
 * Created by sunlu on 17/12/8.
 */
public class InterfaceTest1 {

    public static void main(String[] args) {

        String posFile = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/posdict.txt";
        String negFile = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/sentimentDic/negdict.txt";
        String stopwordsFile = "/Users/sunlu/Documents/workspace/IDEA/SparkDiary/data/Stopwords.dic";


        String input = "今天天气很好";
        sentimentInterface sInter = new sentimentInterface();
        int s = sInter.getSentimentScore(input, posFile, negFile);
        System.out.println(s);

        String input2 = "今天天气很不好";
        int s2 = sInter.getSentimentScore(input2, posFile, negFile);
        System.out.println(s2);

        String input3 = "今天天气不很好";
        int s3 = sInter.getSentimentScore(input3, posFile, negFile);
        System.out.println(s3);
    }
}
