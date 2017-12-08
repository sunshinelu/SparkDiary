package com.evayInfo.Inglory.Project.sentiment.Interface

import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * Created by sunlu on 17/12/8.
 */
class sentimentInterface {

  def getSentimentScore(input:String,posFile:String, negFile:String, stopwordsFile:String): Int = {

    //获取正类、负类词典
    val posWords = Source.fromFile(posFile,"UTF-8").getLines().toList
    val negWords = Source.fromFile(negFile,"UTF-8").getLines().toList

    //load stopwords file
    val stopwords = Source.fromFile(stopwordsFile,"UTF-8").getLines().toList

    // 将正负类添加到词典中
    posWords.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })
    negWords.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })

    //分词、停用词过滤、正类、负类词过滤
    val segWords = ToAnalysis.parse(input).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word))

    // 获取字符串中正负类情感词的数量
    val posScore = segWords.filter(word => posWords.contains(word)).length
    val negScore = segWords.filter(word => negWords.contains(word)).length
    // 获取情感打分
    val sentimentScore = posScore - negScore
    sentimentScore

  }


  def getSentimentScore(input:String,posFile:String, negFile:String): Int = {

    //获取正类、负类词典
    val posWords = Source.fromFile(posFile,"UTF-8").getLines().toList
    val negWords = Source.fromFile(negFile,"UTF-8").getLines().toList


    // 将正负类添加到词典中
    posWords.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })
    negWords.foreach(x => {
      UserDefineLibrary.insertWord(x, "userDefine", 1000)
    })

    //分词、停用词过滤、正类、负类词过滤
    val segWords = ToAnalysis.parse(input).toArray.map(_.toString.split("/")).
      filter(_.length >= 2).map(_ (0)).toList.
      filter(word => word.length >= 1)// & !stopwords.contains(word))

    // 获取字符串中正负类情感词的数量
    val posScore = segWords.filter(word => posWords.contains(word)).length
    val negScore = segWords.filter(word => negWords.contains(word)).length
    // 获取情感打分
    val sentimentScore = posScore - negScore
    sentimentScore

  }

}
