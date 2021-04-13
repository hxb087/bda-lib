package bda.spark.runnable.SVDFeature

import bda.common.Logging
import bda.spark.model.SVDFeature.SVDFeature
import bda.common.obj.SVDFeaturePoint
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Command line runner for SVDFeature Trainer
  */
object Train extends Logging {

  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aks").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("bda.spark.runnable.SVDFeature.Train") {
      head("Local SVDFeature Train", "1.0")
      opt[String]("train_pt").required()
        .text("The path of training file")
        .action { (x, c) => c.copy(train_pt = x) }
      opt[String]("validate_pt")
        .text("The path of validate file")
        .action { (x, c) => c.copy(validate_pt = x) }
      opt[String]("model_pt")
        .text("The path of model file")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Boolean]("is_regression")
        .text(s"Regression or classification, default is ${default_params.is_regression}.")
        .action { (x, c) => c.copy(is_regression = x) }
      opt[Boolean]("graphx")
        .text(s"Whether or not use GraphX, default is ${default_params.graphx}.")
        .action { (x, c) => c.copy(graphx = x) }
      opt[Double]("split_ratio")
        .text(s"The percent of data used for training, default is ${default_params.split_ratio}")
        .action { (x, c) => c.copy(split_ratio = x) }
      opt[Int]("max_iter")
        .text(s"The max number of iterations, default is ${default_params.max_iter}")
        .action { (x, c) => c.copy(max_iter = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --max_iter must > 0")
        }
      opt[Int]("K")
        .text("The reduced rank.")
        .action { (x, c) => c.copy(K = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --K must > 0")
        }
      opt[Int]("n_partition")
        .text(s"Number of partitions, default is ${default_params.n_partition}.")
        .action { (x, c) => c.copy(n_partition = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --n_partition must > 0")
        }
      opt[Double]("reg")
        .text("reg is the regularization coefficient, default if 0.1")
        .action { (x, c) => c.copy(reg = x) }
        .validate { x =>
          if (x >= 0.0) success else failure("Option --reg must >= 0.0")
        }
      opt[Double]("learn_rate")
        .text(s"learning rate, default is ${default_params.learn_rate}")
        .action { (x, c) => c.copy(learn_rate = x) }
        .validate { x =>
          if (x > 0.00) success else failure("Option --learn_rate must > 0.00")
        }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {

    val sparkConf = new SparkConf()
      .setAppName("Spark SVDFeature Training")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val pns = sc.textFile(p.train_pt, p.n_partition).map(SVDFeaturePoint.parse)

    // prepare training and validate datasets
    val (train_pns, valid_pns) = if (!p.validate_pt.isEmpty) {
      val pns2 = sc.textFile(p.validate_pt, p.n_partition).map(SVDFeaturePoint.parse)
      (pns, pns2)
    } else if (p.split_ratio > 0.0 && p.split_ratio < 1) {
      // split points for training and test
      val Array(pns1, pns2) = pns.randomSplit(Array(p.split_ratio, 1 - p.split_ratio))
      (pns1, pns2)
    } else {
      // train without validation
      (pns, null)
    }

    train_pns.cache()
    if (valid_pns != null) train_pns.cache()

    val model = SVDFeature.train(train_pns, valid_pns, p.is_regression,
      p.graphx, p.K, p.learn_rate, p.reg, p.max_iter)

    if (!p.model_pt.isEmpty)
      model.save(p.model_pt)
  }

  /** command line parameters */
  case class Params(train_pt: String = "",
                    validate_pt: String = "",
                    model_pt: String = "",
                    is_regression: Boolean = true,
                    split_ratio: Double = 1.0,
                    K: Int = 20,
                    graphx: Boolean = false,
                    learn_rate: Double = 0.01,
                    reg: Double = 0.01,
                    max_iter: Int = 100,
                    n_partition: Int = 10)
}