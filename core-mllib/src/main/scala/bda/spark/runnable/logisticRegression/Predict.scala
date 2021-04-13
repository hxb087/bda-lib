package bda.spark.runnable.logisticRegression

import bda.common.Logging
import bda.common.obj.LabeledPoint
import bda.spark.model.logisticRegression.LRModel
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Logistic Regression predictor
  * Input:
  * - test_pt   format:fid:v fid:v ...
  * Output:
  * - predict_pt  format: predicted_label
  */
object Predict extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Predict") {
      head("Predict", "1.0")
      opt[String]("model_pt").required()
        .text("The path of model file")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[String]("test_pt").required()
        .text("The path of test file that contains unlabeled data")
        .action { (x, c) => c.copy(test_pt = x) }
      opt[String]("predict_pt").required()
        .text("The path of predict result.")
        .action { (x, c) => c.copy(predict_pt = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Logistic Regression Prediction")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val model: LRModel = LRModel.load(sc, p.model_pt)
    val points = sc.textFile(p.test_pt).map(LabeledPoint.parse).cache()

    val predictions = model.predict(points).zip(points).map {
      case (y, pn) => s"${pn.id}\t${pn.label}\t$y"
    }
    predictions.saveAsTextFile(p.predict_pt)
  }

  /** command line parameters */
  case class Params(model_pt: String = "",
                    test_pt: String = "",
                    predict_pt: String = "")
}
