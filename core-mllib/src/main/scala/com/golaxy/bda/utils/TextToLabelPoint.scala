package com.golaxy.bda.utils

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object TextToLabelPoint {
  def create_label_point(line:String,maxlength:Int):LabeledPoint = {
    //字符串去空格，以逗号分隔转为数组
    val linearr = line.trim().split(" ")
//    if(linearr==null || linearr.length==0){
//    }

    //定长数组转可变数组
    val linearrbuff = linearr.toBuffer
    //移除label元素（将linedoublearr的第一个元素作为标签）
    //将剩下的元素转为向量
    val (indexes, values) = linearrbuff.filter(f=>f.contains(":")).map{
      kv=>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toDouble)
    }.unzip

    val vector = Vectors.sparse(maxlength,indexes.toArray,values.toArray)
    //返回标签向量
    LabeledPoint(1.0,vector)
  }
  def create_label_point(label:String,line:String,maxlength:Int):LabeledPoint = {
    //字符串去空格，以逗号分隔转为数组
    val linearr = line.trim().split(" ")
    //    if(linearr==null || linearr.length==0){
    //    }

    //定长数组转可变数组
    val linearrbuff = linearr.toBuffer
    //移除label元素（将linedoublearr的第一个元素作为标签）
    //将剩下的元素转为向量
    val (indexes, values) = linearrbuff.filter(f=>f.contains(":")).map{
      kv=>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toDouble)
    }.unzip

    val vector = Vectors.sparse(maxlength,indexes.toArray,values.toArray)
    var labeltext = label
    if(label == null || "".equals(label)){
      labeltext = "0"
    }
    //返回标签向量
    LabeledPoint(labeltext.toDouble,vector)
  }

  /**
    *
    * @param line
    * @return
    *         获得向量特征长度
    */
  def maxSize(line:String):Int = {
    val linearr = line.trim().split(" ")
    //    if(linearr==null || linearr.length==0){
    //    }

    //定长数组转可变数组
    val linearrbuff = linearr.toBuffer
    //移除label元素（将linedoublearr的第一个元素作为标签）
    //将剩下的元素转为向量
    val (indexes, values) = linearrbuff.filter(f=>f.contains(":")).map{
      kv=>
        val Array(k, v) = kv.split(":")
        var bb = (0,0.0)
        try{
          bb = (k.toInt, v.toDouble)
        }catch{
          case e: Exception => println(e + kv)
        }

        bb
    }.unzip

    indexes.max+1
  }
  def create_vector(line:String,maxlength:Int): org.apache.spark.ml.linalg.Vector = {

//    println(s"line = ${line}")
    //字符串去空格，以逗号分隔转为数组
    val linearr = line.trim().split(" ")


    //定长数组转可变数组
    val linearrbuff = linearr.toBuffer
    //移除label元素（将linedoublearr的第一个元素作为标签）
    //将剩下的元素转为向量
    val (indexes, values) = linearrbuff.filter(f=>f.contains(":")).map{
      kv=>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toDouble)
    }.distinct.sortBy(s=>s._1).unzip

    val vector = org.apache.spark.ml.linalg.Vectors.sparse(maxlength,indexes.toArray,values.toArray)

    //返回标签向量
    vector
  }


}
