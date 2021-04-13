package com.golaxy.bda.utils

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Administrator on 2018/9/12.
  */
object LabeledPointUtils {


  /**
    * 将字符串转换为LabeledPoint标签向量
    * @param s
    *          0 1:2 2:3 4:5
    * @param split
    * @return
    */
   def toLabeledPoint(s : String,split : String ): LabeledPoint = {

     val sArr = s.split(split)
     val label = sArr.head.toDouble
    val (indices, values) = sArr.tail.map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip
    val indicesLength = indices.length

     LabeledPoint(label,Vectors.sparse(indicesLength,indices,values))

   }
}
