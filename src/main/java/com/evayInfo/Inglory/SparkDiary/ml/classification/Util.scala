package com.evayInfo.Inglory.SparkDiary.ml.classification

public class Util {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  private def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }


}
