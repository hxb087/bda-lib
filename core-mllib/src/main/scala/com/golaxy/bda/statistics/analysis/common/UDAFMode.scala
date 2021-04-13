package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * @author ：ljf
 * @date ：2020/10/10 17:10
 * @description：mode statistics analysis
 * @modified By：
 * @version: $ 1.0
 */
class UDAFMode extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType(StructField("inputStr", StringType, true) :: Nil)
  }


  override def bufferSchema: StructType = {
    StructType(StructField("bufferMap", MapType(keyType = StringType, valueType = IntegerType), true) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = false

  //initial map
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = scala.collection.immutable.Map[String, Int]()
  }

  //update key value
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val key = input.getAs[String](0)
    val string2Int: Map[String, Int] = buffer.getAs[Map[String, Int]](0)
    val bufferMap = scala.collection.mutable.Map[String, Int](string2Int.toSeq: _*)
    val ret = if (bufferMap.contains(key)) {
      val new_value = bufferMap(key) + 1
      bufferMap.put(key, new_value)
      bufferMap
    } else {
      bufferMap.put(key, 1)
      bufferMap
    }
    buffer.update(0, ret)

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //same key, add values
    val tempMap = (buffer1.getAs[Map[String, Int]](0) /: buffer2.getAs[Map[String, Int]](0)) {
      case (map, (k, v)) => map + (k -> (v + map.getOrElse(k, 0)))
    }
    buffer1.update(0, tempMap)
  }

  override def evaluate(buffer: Row): Any = {
    //get key correspond max value
    var maxValue = 0
    var result = ""
    buffer.getAs[Map[String, Int]](0).foreach({ x =>
      val key = x._1
      val value = x._2
      if (value > maxValue) {
        maxValue = value
        result = key
      }
    })
    result.toDouble
  }
}

