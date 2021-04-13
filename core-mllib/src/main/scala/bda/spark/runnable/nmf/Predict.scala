package bda.spark.runnable.NMF

import bda.common.Logging
import bda.common.obj.Rate
import bda.spark.model.matrixFactorization.NMFModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * NMF predictor
  * Input:
  * - test_pt   format:i j
  * Output:
  * - predict_pt  format: i j v
  */
object Predict extends Logging {

  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("NMFPredict") {
      head("NMFPredict", "1.0")
      opt[String]("model_pt")
        .required()
        .text("The path of model file")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[String]("test_pt")
        .required()
        .text("The path of test file that contains unlabeled data")
        .action { (x, c) => c.copy(test_pt = x) }
      opt[String]("predict_pt")
        .required()
        .text("The path of predict result.")
        .action { (x, c) => c.copy(predict_pt = x) }
      opt[Boolean]("graphx")
        .required()
        .text("If true, use the graphx implementation; Else use the shared model implementation. Default is .")
        .action { (x, c) => c.copy(graphx = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(p: Params) {
    val sparkConf = new SparkConf()
      .setAppName("Spark NMF Prediction")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val model: NMFModel = NMFModel.load(sc, p.model_pt, p.graphx)
    val rates: RDD[Rate] = sc.textFile(p.test_pt).map(Rate.parse)
    val predictions: RDD[(Double, Rate)] = model.predict(rates)

    predictions.map {
      case (prediction, rate) => s"${rate.user} ${rate.item}\t${rate.label}\t$prediction"
    }.saveAsTextFile(p.predict_pt)
  }

  /** command line parameters */
  case class Params(model_pt: String = "",
                    test_pt: String = "",
                    predict_pt: String = "",
                    graphx: Boolean = false)
}
