package com.golaxy.bda.utils

import breeze.numerics.exp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
 * Created by YuSihao on 2018/9/18.
 */
object LabeledPointUtilsWithKernel {


  /**
   * 将字符串转换为LabeledPoint标签向量
   *
   * @param s
   * 0 1:2 2:3 4:5
   * @param split
   * @return
   */
  def toLabeledPoint(s: String, split: String, kernel: String, length: Int): LabeledPoint = {

    val sArr = s.split(split)
    val label = sArr.head.toDouble
    val (indices, values) = sArr.tail.map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = datakernel(indexAndValue(1).toDouble, kernel)
      (index, value)
    }.unzip
    val indicesLength = length

    LabeledPoint(label, Vectors.sparse(indicesLength, indices, values))

  }

  def max(a: Int, b: Int): Int = {
    if (a > b) a else b
  }

  def countLength(s: String, split: String): Int = {
    val sArr = s.split(split)
    val (indices, values) = sArr.tail.map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt // Convert 1-based indices to 0-based.
      (index, 0)
    }.unzip
    val indicesLength = indices.reduce(max)
    indicesLength
  }

  def toLabeledPoint(s: String, split: String, kernel: String, length: Int, classid: Double): LabeledPoint = {

    val sArr = s.split(split)
    var label = sArr.head.toDouble
    if (label == classid) label = 1.0
    else label = 0.0
    val (indices, values) = sArr.tail.map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = datakernel(indexAndValue(1).toDouble, kernel)
      (index, value)
    }.unzip
    val indicesLength = length

    LabeledPoint(label, Vectors.sparse(indicesLength, indices, values))

  }

  /**
   * 将DataFrame的row类型转为LabelPoint，输入为DenseVector
   *
   * @param row     ：每一行数据
   * @param kernel  ：核函数
   * @param classId ：当前样本所属类别id
   * @return
   */
  def toLabeledPoint(row: Row, kernel: String, classId: Double): LabeledPoint = {
    var label = row.getInt(row.length - 1).toDouble
    if (label == classId) label = 1.0
    else label = 0.0

    //前n-1个元素为数据，最后一个元素为label
    val values = new Array[Double](row.length - 1)
    for (i <- 0 until row.length - 1) {
      values(i) = row.getDouble(i)
    }
    LabeledPoint(label, Vectors.dense(values))
  }

  /**
   * svm model version2的预测数据处理
   *
   * @param row    ：Row[features... label]
   * @param kernel ：String 核函数指定
   * @return
   */
  def toLabeledPoint(row: Row, kernel: String): LabeledPoint = {
    val label = row.getInt(row.length - 1).toDouble

    //前n-1个元素为数据，最后一个元素为label
    val values = new Array[Double](row.length - 1)
    for (i <- 0 until row.length - 1) {
      values(i) = row.getDouble(i)
    }
    LabeledPoint(label, Vectors.dense(values))
  }

  def datakernel(data: Double, kernel: String): Double = {
    val res: Double = kernel match {
      case "None" => data
      case "Sigmoid" => 1.0 / (1 + exp(-data))
      case "Polynomial" => data * data
    }
    return res
  }
}
