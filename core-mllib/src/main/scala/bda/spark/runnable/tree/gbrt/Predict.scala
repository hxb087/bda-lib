package bda.spark.runnable.tree.gbrt

import bda.common.obj.LabeledPoint
import bda.spark.model.tree.gbrt.GBRTModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
  * Command line runner for spark GBRT(Gradient Boosting Regression Trees).
  *
  * Input:
  * - train_pt format: label fid1:v1 fid2:v2 ...
  * Both label and v are doubles, fid are integers starting from 1.
  */
object Predict {

  /** command line parameters */
  case class Params(test_pt: String = "",
                    model_pt: String = "",
                    predict_pt: String = "")

  def main(args: Array[String]) {
    // do not show log info
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkGBRTPredict") {
      head("RunSparkGBRTPredict: an example app for GBRT.")
      opt[String]("test_pt").required()
        .text("input path of the training data set with LabeledPoint format")
        .action((x, c) => c.copy(test_pt = x))
      opt[String]("model_pt").required()
        .text("directory of the GBRT model")
        .action((x, c) => c.copy(model_pt = x))
      opt[String]("predict_pt").required()
        .text(s"output path of the prediction")
        .action((x, c) => c.copy(predict_pt = x))
      note(
        """
          |For example, the following command runs this app on your data set:
          |
          | bin/spark-submit --class bda.runnable.tree.gbrt.Predict \
          |   out/artifacts/*/*.jar \
          |   --train_pt ... \
          |   --model_pt ... \
          |   --predict_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark GBRT Prediction")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    // prepare test data and model
    val test = sc.textFile(params.test_pt).map(LabeledPoint.parse).cache()
    val model = GBRTModel.load(sc, params.model_pt)
    // predict for test data
    val predict = model.predict(test)
    // save on disk
    predict.map(e => s"${e._1}\t${e._2}\t${e._3}").saveAsTextFile(params.predict_pt)

  }
}