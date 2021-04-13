package com.golaxy.bda.evaluate

import bda.spark.evaluate.Classification
import com.golaxy.bda.utils.DFUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import bda.common.util.Msg
import bda.common.Logging


/**
 * Evaluate for multiple classification.
 *
 * Input:
 * - predict_pt format: predict label fid1:v1 fid2:v2 ...
 * predict, label and v are doubles, fid are integers starting from 1.
 */
object MultiClassificationRunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkEvaluateMultipleClassification") {
      head("RunSparkEvaluateMultipleClassification: an example app for evaluation on your multiple classification.")
      opt[String]("input_pt")
        .required()
        .text("directory of the prediction result")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("label_pt")
        .required()
        .text("label_pt")
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
          | bin/spark-submit --class bda.runnable.evaluate.MultiClassification \
          |   out/artifacts/*/*.jar \
          |   --predict_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val spark = SparkSession.builder()
      .appName("MultiClassificationRunner")
//    .master("local")
      .getOrCreate()

    /**读入文件*/
    val df = DFUtils.loadcsv(spark,params.input_pt)

    /**   选择要处理的数据*/
    val rdd= df.select(params.label_pt, params.prediction_pt).rdd
    val lps: RDD[(Double, Double)] = rdd.map{line=>
      var Array(label,prediction) = line.toString().replace("[","").replace("]","").split(",")
      (label.toDouble,prediction.toDouble)
    }.cache()

    val acc = Classification.accuracy(lps)
    val msg = Msg("accuracy" -> acc)
    logInfo(msg.toString)

    /** get  DataFrame accuracyDF */
    val accuracyDF = spark.createDataFrame(Seq((1, acc))) toDF("id", "accuracy")
//    accuracyDF.show()

    DFUtils.exportcsv(accuracyDF,params.output_pt)
    spark.stop()
  }

  /** command line parameters */
  case class Params(
                     input_pt: String = "D:\\testdata\\evaluate\\MultiClassificationRunner\\input_pt\\result.csv",
                     label_pt: String = "quality",
                     prediction_pt: String = "quality_pre",
                     output_pt: String = "D:\\testdata\\evaluate\\MultiClassificationRunner\\output"
                   )
}