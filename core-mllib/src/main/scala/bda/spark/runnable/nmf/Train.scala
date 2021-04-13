package bda.spark.runnable.NMF

import bda.common.Logging
import bda.common.obj.Rate
import bda.spark.model.matrixFactorization.NMF
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Command line runner for logistic regression
  * Input:
  * - train_pt    format: i j v
  */
object Train extends Logging {

  /** Parse the command line parameters */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("NFMTrain") {
      head("NMFTrain", "1.0")
      opt[String]("train_pt")
        .required()
        .text("The path of training file")
        .action { (x, c) => c.copy(train_pt = x) }
      opt[String]("validate_pt")
        .text("The path of validate file")
        .action { (x, c) => c.copy(validate_pt = x) }
      opt[String]("model_pt")
        .text("The path of model to store")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Boolean]("graphx")
        .text(s"Whether or not use GraphX, default is ${default_params.graphx}")
        .action { (x, c) => c.copy(graphx = x) }
      opt[Double]("split_ratio")
        .text(s"The percent of data used for training, default is ${default_params.split_ratio}")
        .action { (x, c) => c.copy(split_ratio = x) }
        .validate { x =>
          if (x >= 0 && x <= 1) success else failure("Option --split_ratio must >= 0 and =< 1")
        }
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
      opt[Double]("reg")
        .text(s"reg is the regularization coefficient, default if ${default_params.reg}")
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
      opt[Int]("n_partition")
        .text(s"The min number of RDD partitions, default value is SparkContext.defaultMinPartitions.")
        .action { (x, c) => c.copy(n_partition = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(p: Params) {

    val sparkConf = new SparkConf()
      .setAppName("Spark NMF Training")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val n_partition = if (p.n_partition > 0)
      p.n_partition
    else
      sc.defaultMinPartitions

    val rds = sc.textFile(p.train_pt, n_partition).map(Rate.parse)

    // prepare training and validate datasets
    val (train_rds, valid_rds) = if (!p.validate_pt.isEmpty) {
      val rds2 = sc.textFile(p.validate_pt, n_partition).map(Rate.parse)
      (rds, rds2)
    } else if (p.split_ratio > 0.0 && p.split_ratio < 1) {
      // split points for training and test
      val Array(rds1, rds2) = rds.randomSplit(Array(p.split_ratio, 1 - p.split_ratio))
      (rds1, rds2)
    } else {
      // train without validation
      (rds, null)
    }

    train_rds.cache()
    if (valid_rds != null) valid_rds.cache()

    val model = NMF.train(train_rds, valid_rds, p.K,
      p.graphx, p.learn_rate, p.reg, p.max_iter)

    if (!p.model_pt.isEmpty)
      model.save(p.model_pt)
  }

  /** command line parameters */
  case class Params(train_pt: String = "",
                    validate_pt: String = "",
                    model_pt: String = "",
                    n_partition: Int = -1,
                    graphx: Boolean = false,
                    split_ratio: Double = 1.0,
                    K: Int = 10,
                    learn_rate: Double = 0.01,
                    reg: Double = 0.01,
                    max_iter: Int = 100)
}