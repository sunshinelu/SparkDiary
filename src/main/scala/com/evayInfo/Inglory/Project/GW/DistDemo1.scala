package com.evayInfo.Inglory.Project.GW

import breeze.linalg.DenseMatrix

/*
参考链接：
基于编辑距离来判断词语相似度方法（scala版）
https://www.cnblogs.com/xing901022/p/8028911.html
 */
object DistDemo1 {

  def editDist(s1:String, s2:String):Int ={
    val s1_length = s1.length+1
    val s2_length = s2.length+1

    val matrix = DenseMatrix.zeros[Int](s1_length, s2_length)
    for(i <- 1.until(s1_length)){
      matrix(i,0) = matrix(i-1, 0) + 1
    }

    for(j <- 1.until(s2_length)){
      matrix(0,j) = matrix(0, j-1) + 1
    }

    var cost = 0
    for(j <- 1.until(s2_length)){
      for(i <- 1.until(s1_length)){
        if(s1.charAt(i-1)==s2.charAt(j-1)){
          cost = 0
        }else{
          cost = 1
        }
        matrix(i,j)=math.min(math.min(matrix(i-1,j)+1,matrix(i,j-1)+1),matrix(i-1,j-1)+cost)
      }
    }
    matrix(s1_length-1,s2_length-1)
  }


  def main(args: Array[String]): Unit = {

    val str1 = "这句话和下面那句很像，我就是下面那句话"
//    val str2 = "这句话和下面那句很像，我就是下面那句话"
    val str2 = "我就是下面那句话，这句话和下面那句很像"//"我就是下面那句话"

    val simi = editDist(str1,str2)
    println(s"相似性打分为：$simi") //相似性打分为：12

  }
}
