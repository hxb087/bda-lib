package com.golaxy.bda.statistics.analysis

import java.util

import bda.common.util.{AppUtils, DFUtils}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
 * @author ：ljf
 * @date ：2020/10/14 10:39
 * @description：deviation analysis
 *                        include "z-score","skewness","kurtosis"
 * @modified By：
 * @version: $ 1.0
 */

/**
 * @author ：huxb
 * @date ：2020/11/16 10:39
 * @description：deviation analysis
 *                        include "z-score","skewness","kurtosis"
 * @modified By：add output
 * @version: $ 1.1
 */

object DeviationAnalysis {

  case class Params(inputDataPath: String = "",
                    inputCols: String = "",
                    metrics: String = "",
                    outputPath_metrics: String = "",
                    outputPath_ZScore: String = ""
                   )

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
      opt[String]("outputPath_metrics")
        .required()
        .text("the path of output data")
        .action((x, c) => c.copy(outputPath_metrics = x))
      opt[String]("outputPath_ZScore")
        .required()
        .text("the path of output data")
        .action((x, c) => c.copy(outputPath_ZScore = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(AppUtils.AppName("DeviationAnalysis"))
      .getOrCreate()
    //    spark.udf.register("mode", new UDAFMode)
    //    spark.udf.register("median", new UDAFMedian)
    val rawDF = DFUtils.loadcsv(spark, p.inputDataPath)

    //TODO: mean,median,mode,Quantile
    val inputCols = p.inputCols.split(",")
    val metrics = p.metrics.split(",") ++ Array("mean", "std")

    val colAndMetric = new ArrayBuffer[(String, String)]()
    metrics.map(metric => inputCols.map(col => colAndMetric.append((col, metric))))

    val metricsDF: DataFrame = rawDF.agg(colAndMetric.head, colAndMetric.tail: _*)
    //    metricsDF.show(false)

    //创建输出格式的DataFrame
    val size = metricsDF.columns.size
    val rows = new util.ArrayList[Row]()
    val metricArray = metricsDF.first().toSeq
    for (i <- 0 until size by inputCols.size) {
      rows.add(Row.fromSeq(metricArray.slice(i, i + inputCols.size)))
    }
    val schema = StructType(inputCols.map(filedName => StructField(filedName, DoubleType, true)))
    val resultDF = spark.createDataFrame(rows, schema)
    resultDF.show(false)


    import com.golaxy.bda.statistics.analysis.common.Implicit._

    resultDF.addColumn(metrics, "metrics", StringType).show(false)
    val resultDFNew = resultDF.addColumn(metrics, "metrics", StringType)
    DFUtils.exportcsv(resultDFNew, p.outputPath_metrics)
    //
    //    val array = resultDF.stat.approxQuantile(Array("V0", "V1"), Array(0.25, 0.5, 0.75), 0.01)
    //    for (elem <- array) {
    //      println(elem.toBuffer)
    //    }

    //    rawDF.addZScore("V0","V1","V2","V3").show(false)
    rawDF.addZScore(inputCols.head, inputCols.tail: _*).show(false)
    val rawDFZScore=rawDF.addZScore(inputCols.head, inputCols.tail: _*)
    DFUtils.exportcsv(rawDFZScore, p.outputPath_ZScore)
  }
}
