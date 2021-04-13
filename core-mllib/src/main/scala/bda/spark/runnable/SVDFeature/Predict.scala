package bda.spark.runnable.SVDFeature

import bda.common.util.Msg
import bda.spark.model.SVDFeature.SVDFeatureModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import bda.common.obj.SVDFeaturePoint
import bda.spark.evaluate.Classification.accuracy
import bda.spark.evaluate.Regression.RMSE
import bda.common.Logging
import org.apache.log4j.{Level, Logger}

/**
  * Command line runner for local SVDFeature Predict
  *
  * Input
  * - test_pt  format: uf1:v uf2:v ...,if1:v if2:v ...,gf1:v gf2:v ...
  * - model_pt  binary SVDFeature model
  * Output
  * - prediciton_pt   format: uf1:v uf2:v ...,if1:v if2:v ...,gf1:v gf2:v ...
  */
object Predict extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Spark SVDFeature Predict") {
      head("Predict", "1.0")
      opt[String]("test_pt").required()
        .action { (x, c) => c.copy(test_pt = x) }
        .text("The path of test file. Each line is a point for prediction, with the format:fu:v fu:v ...,fi:v fi:v...,fg:v fg:v ...")
      opt[String]("model_pt").required()
        .action { (x, c) => c.copy(model_pt = x) }
        .text("The path of model file")
      opt[String]("predict_pt").required()
        .action { (x, c) => c.copy(predict_pt = x) }
        .text("The path of prediction file")
      opt[Int]("n_partition")
        .action { (x, c) => c.copy(n_partition = x) }
        .text("number of partitions")
      note(
        """
          |For example, the following command runs this app on your data set:
          |
          | java -jar out/artifacts/*/*.jar \
          |   --train_pt /user/houjp/data/YourTrainingData/
          |   --valid_pt /user/houjp/data/YourValidationData/
          |   --model_pt /user/houjp/model/YourModelName/
          |   --n_partition 100
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark SVDFeature Prediction")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val pns: RDD[SVDFeaturePoint] = sc.textFile(params.test_pt).map(SVDFeaturePoint.parse).cache()
    val model = SVDFeatureModel.load(sc, params.model_pt)
    val tys = model.predictWithLabel(pns)

    val msg = Msg("n(test)" -> tys.count())
    if (model.is_regression)
      msg.append("RMSE", RMSE(tys))
    else
      msg.append("accuracy", accuracy(tys))
    logInfo(msg.toString)

    if (params.predict_pt.nonEmpty)
      tys.zip(pns.map(_.id)).map{
        case ((t, y), id) => s"$id\t$t\t$y"
      }.saveAsTextFile(params.predict_pt)
  }

  /** command line parameters */
  case class Params(test_pt: String = "",
                    model_pt: String = "",
                    predict_pt: String = "",
                    n_partition: Int = 10)
}
