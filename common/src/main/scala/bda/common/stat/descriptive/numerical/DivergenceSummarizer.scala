package org.apache.spark.ml.stat.descriptive.numerical

//package bda.common.stat.descriptive.numerical

import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

/**
 * @author ：ljf
 * @date ：2020/9/22 17:40
 * @description：numerical summarizer
 * @modified By：
 * @version: $ 1.0
 */
object DivergenceSummarizer extends Summarizer {

  def setGroupsNumber(groupsNumber: Int): Unit = {
    this.groupsNumber = groupsNumber
  }

  def getGroupsNumber(): Int = {
    groupsNumber
  }

  def mean(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "mean")
  }


  def mean(col: Column): Column = mean(col, lit(1.0))


  def sum(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "sum")
  }


  def sum(col: Column): Column = sum(col, lit(1.0))


  def variance(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "variance")
  }


  def variance(col: Column): Column = variance(col, lit(1.0))


  def std(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "std")
  }


  def std(col: Column): Column = std(col, lit(1.0))


  def count(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "count")
  }

  def count(col: Column): Column = count(col, lit(1.0))

  def numNonZeros(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "numNonZeros")
  }

  def numNonZeros(col: Column): Column = numNonZeros(col, lit(1.0))


  def max(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "max")
  }


  def max(col: Column): Column = max(col, lit(1.0))


  def min(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "min")
  }


  def min(col: Column): Column = min(col, lit(1.0))


  def normL1(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "normL1")
  }


  def normL1(col: Column): Column = normL1(col, lit(1.0))


  def normL2(col: Column, weightCol: Column): Column = {
    getSingleMetric(col, weightCol, "normL2")
  }


  def normL2(col: Column): Column = normL2(col, lit(1.0))

  private def getSingleMetric(col: Column, weightCol: Column, metric: String): Column = {
    val c1 = metrics(metric).summary(col, weightCol)
    c1.getField(metric).as(s"$metric($col)")
  }

}

