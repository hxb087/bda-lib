package com.golaxy.bda.utils

import org.apache.spark.ml.param.{Param, Params}

trait MyTransformEstimatorParams extends Params{
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  def setInputCol(value: String):this.type = set(inputCol, value)
  def setOutputCol(value: String):this.type = set(outputCol, value)
  def getInputCol: String = ${inputCol}
  def getOutputCol: String = ${outputCol}
}
