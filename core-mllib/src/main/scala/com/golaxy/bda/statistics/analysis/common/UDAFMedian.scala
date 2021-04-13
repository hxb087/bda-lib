package com.golaxy.bda.statistics.analysis.common

import java.util
import java.util.Collections

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * @author ：ljf
 * @date ：2020/10/13 10:17
 * @description：求中位数的聚合函数
 * @modified By：
 * @version: $ 1.0
 */
class UDAFMedian extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("input", StringType) :: Nil)
  //FIXME: bufferSchema应该为StringType？
  override def bufferSchema: StructType = StructType(StructField("sum", StringType) :: StructField("count", StringType) :: Nil)
  override def dataType: DataType = DoubleType
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.get(0) + "," + input.get(0)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.get(0) + "," + buffer2.get(0))
  }

  override def evaluate(buffer: Row): Any = {
    val list = new util.ArrayList[java.lang.Double]
    val stringList:Array[String] = buffer.getString(0).split(",")
    for (elem <- stringList) {
      if(StringUtils.isNotBlank(elem)){
        list.add(elem.toDouble)
      }
    }
    Collections.sort(list)
    val size = list.size
    var num:Double = 0L
    if (size % 2 == 1) num = list.get(((size+1) / 2) - 1).toDouble
    if (size % 2 == 0) num = (list.get(size / 2 - 1) + list.get(size / 2)) / 2.00
    num
  }
}
