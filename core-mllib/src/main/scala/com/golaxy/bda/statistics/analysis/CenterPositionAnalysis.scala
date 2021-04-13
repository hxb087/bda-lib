package com.golaxy.bda.statistics.analysis

import java.util

import bda.common.util.{AppUtils, DFUtils}
import com.golaxy.bda.statistics.analysis.common.UDAFMedian
import org.apache.spark.sql.catalyst.expressions.aggregate.UDAFMode
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


/**
 * @author ：ljf
 * @date ：2020/10/10 14:14
 * @description：
 * example: --metrics mode,quantile --inputDataPath data/mllib/iris_wheader.csv
 * --outputPath out/credit/CenterPosition --inputCols sepal_len,sepal_wid
 * @modified By：
 * @version: $ 1.0
 */
object CenterPositionAnalysis {

  case class Params(inputDataPath: String = "",
                    inputCols: String = "",
                    metrics: String = "",
                    outputPath: String = "")

  def main(args: Array[String]): Unit = {
    val default_params: Params = Params()
    val parser = new OptionParser[Params]("CenterPositionAnalysis") {
      head("CenterPositionAnalysis", "2.0")
      opt[String]("inputDataPath")
        .required()
        .text("Test Input file path")
        .action((x, c) => c.copy(inputDataPath = x))
      opt[String]("inputCols")
        .required()
        .text("Input columns name")
        .action((x, c) => c.copy(inputCols = x))
      opt[String]("metrics")
        .required()
        .text("the metrics name")
        .action((x, c) => c.copy(metrics = x))
      opt[String]("outputPath")
        .required()
        .text("the path of output data")
        .action((x, c) => c.copy(outputPath = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(AppUtils.AppName("CenterPositionAnalysis"))
      .getOrCreate()
    spark.udf.register("mode", new UDAFMode)
    spark.udf.register("median", new UDAFMedian)
    val rawDF = DFUtils.loadcsv(spark, p.inputDataPath)

    //TODO: 是否支持单个指标查询，鲁棒性
    val quantileName = "quantile"
    val inputCols = p.inputCols.split(",")
    val metrics = p.metrics.split(",")

    val colAndMetric = new ArrayBuffer[(String, String)]()
    //调用spark自带的agg函数，输入格式[<col,metricName>]
    metrics.filter(_ != quantileName).map(metric => inputCols.map(col => colAndMetric.append((col, metric))))
    val metricsDF: DataFrame = rawDF.agg(colAndMetric.head, colAndMetric.tail: _*)

    //create output format DataFrame
    val size = metricsDF.columns.length
    val rows = new util.ArrayList[Row]()
    val metricSeq = metricsDF.first().toSeq
    for (i <- 0 until size by inputCols.size) {
      rows.add(Row.fromSeq(metricSeq.slice(i, i + inputCols.size)))
    }
    val schema = StructType(inputCols.map(filedName => StructField(filedName, DoubleType, true)))

    //add quantile
    val hasQuantile = metrics.contains(quantileName)
    var metricsCol = metrics.filter(_ != quantileName)
    if (hasQuantile) {
      val probabilities = Array(0.25, 0.5, 0.75, 1.0)
      // 矩阵的转置 [columns, probabilities] -> [probabilities, columns]
      val quantileArray = rawDF.stat.approxQuantile(inputCols, probabilities, 0.01)
      for (i <- probabilities.indices) {
        val buffer = new ArrayBuffer[Double]()
        quantileArray.foreach(elem => buffer.append(elem(i)))
        rows.add(Row.fromSeq(buffer))
      }

      //add quantile schema
      metricsCol ++= probabilities.map(pro => quantileName + s"-${pro}")
    }

    var resultDF = spark.createDataFrame(rows, schema)

    //添加metrics 列名
    import com.golaxy.bda.statistics.analysis.common.Implicit._
    resultDF = resultDF.addColumn(metricsCol, "metrics", StringType)

    DFUtils.exportcsv(resultDF, p.outputPath)
    spark.stop()
  }


}
