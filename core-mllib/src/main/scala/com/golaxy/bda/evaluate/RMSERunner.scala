package com.golaxy.bda.evaluate

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructType, DoubleType, StructField}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import bda.common.Logging
import org.apache.spark.mllib.evaluation.{RegressionMetrics, RankingMetrics}
import scala.collection.mutable.ArrayBuffer


/**
  * Evaluate for binary classification.
  *
  * 2020/07/16
  * 修改从csv读入和读出
  * 使用mllib的库函数
  *
  */
object RMSERunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RMSERunner") {
      head("RMSERunner: an example app for evaluation on regression.")
      opt[String]("input_pt")
        .required()
        .text("Prediction file path.")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("label_pt")
        .required()
        .text("label_pt.")
        .action((x, c) => c.copy(label_pt = x))
      opt[String]("prediction_pt")
        .required()
        .text("prediction_pt")
        .action((x, c) => c.copy(prediction_pt = x))
      opt[String]("output_pt")
        .required()
        .text("output_pt")
        .action((x, c) => c.copy(output_pt = x))
      note(
        """
          |For example, the following command runs this app on your predictions:
          |
          | bin/spark-submit --class bda.runnable.evaluate.RMSERunner \
          |  spark.jar --predict_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val spark = SparkSession.builder()
      .appName("RMSE")
//    .master("local")
      .getOrCreate()
    /** read csv File*/
    val df = DFUtils.loadcsv(spark,params.input_pt)

    /**choose your collumn*/
    val rdd= df.select(params.label_pt, params.prediction_pt).rdd

    val predictedAndTrue = rdd.map(line => {
      (line(0).toString.toDouble, line(1).toString.toDouble)
  }).cache()

    val regressionMetrics: RegressionMetrics = new RegressionMetrics(predictedAndTrue)
//    println("MSE：" + regressionMetrics.meanSquaredError)
//    println("RMSE：" + regressionMetrics.rootMeanSquaredError)

    /**构建rdd转dataframe的格式*/
    val sc = spark.sparkContext
    val metricsBuff = new ArrayBuffer[Row]()
    val structFields = Array(StructField("MSE",DoubleType,true),StructField("RMSE",DoubleType,true))
    val structType = StructType(structFields)
    metricsBuff.append(Row(regressionMetrics.meanSquaredError,regressionMetrics.rootMeanSquaredError))
    val metricsBuffrdd = sc.parallelize(metricsBuff).repartition(1)
    val metricsBuffDF = spark.createDataFrame(metricsBuffrdd,structType)
    metricsBuffDF.show()

    DFUtils.exportcsv(metricsBuffDF,params.output_pt)
    spark.stop()
  }

  /** command line parameters */
  case class Params(
              input_pt: String = "D:\\testdata\\evaluate\\RMSERunner\\input\\result.csv",
              label_pt: String = "quality",
              prediction_pt: String = "quality_pre",
              output_pt: String = "D:\\testdata\\evaluate\\RMSERunner\\output"
  )
}