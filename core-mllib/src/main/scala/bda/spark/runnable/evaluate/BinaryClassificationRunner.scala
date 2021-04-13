package bda.spark.runnable.evaluate

import bda.spark.evaluate.Classification
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import bda.common.util.Msg
import bda.common.Logging


/**
 * Evaluate for binary classification.
 *
 * Input:
 * - predict_pt format: predict label fid1:v1 fid2:v2 ...
 * predict, label and v are doubles, fid are integers starting from 1.
 */
object BinaryClassificationRunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("BinaryClassificationRunner") {
      head("BinaryClassificationRunner: an example app for evaluation on your binary classification.")
      opt[String]("predict_pt")
        .required()
        .text("directory of the prediction result")
        .action((x, c) => c.copy(predict_pt = x))
      note(
        """
          |For example, the following command runs this app on your predictions:
          |
          | bin/spark-submit --class bda.runnable.evaluate.BinaryClassification \
          |  spark.jar --predict_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Spark Evaluation of Binary Classification")
    val sc = new SparkContext(conf)

    val lps = sc.textFile(params.predict_pt).map { ln =>
      val items = ln.trim.split("\t")
      assert(items.size > 2, "input format should be: id  label prediction")
      val prediction = items(2).toDouble
      val label = items(1).toDouble
      (label, prediction)
    }.cache()

    val acc = Classification.accuracy(lps)
    val pre = Classification.precision(lps)
    val rec = Classification.recall(lps)
    val auc = Classification.auc(lps)

    val msg = Msg("n(prediction)" -> lps.count(),
      "accuracy" -> acc,
      "precision" -> pre,
      "recall" -> rec,
      "auc" -> auc)

    println(msg.toString)
  }

  /** command line parameters */
  case class Params(predict_pt: String = "")
}