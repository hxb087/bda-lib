package bda.spark.runnable.logisticRegression

import bda.common.Logging
import bda.common.obj.LabeledPoint
import bda.spark.model.logisticRegression.{LR, LRModel}

//import bda.spark.preprocess.Points
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Command line runner for spark (half-distributed) logistic regression
  * Input:
  * - train_pt    format: label    fid:v fid:v ...
  * Both label and fid are integers starting from 0
  */
object Train extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Spark Logistic regression Train") {
      head("Train", "1.0")
      opt[String]("train_pt").required()
        .action { (x, c) => c.copy(train_pt = x) }
        .text("The path of training file")
      opt[String]("validate_pt")
        .action { (x, c) => c.copy(validate_pt = x) }
        .text("The path of validate file")
      opt[String]("model_pt")
        .text("The path of model to store")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Double]("split_ratio")
        .action { (x, c) => c.copy(split_ratio = x) }
        .text(s"The percent of data used for training, default is ${default_params.split_ratio}")
        .validate { x =>
          if (x >= 0 && x <= 1) success else failure("Option --split_ratio must >= 0 and <= 1")
        }
      opt[Boolean]("graphx")
        .action { (x, c) => c.copy(graphx = x) }
        .text(s"Whether or not use GraphX, default is ${default_params.graphx}.")
      opt[Int]("max_iter")
        .action { (x, c) => c.copy(max_iter = x) }
        .text(s"The max number of iterations, default is ${default_params.max_iter}")
        .validate { x =>
          if (x > 0) success else failure("Option --max_iter must > 0")
        }
      opt[Double]("reg")
        .action { (x, c) => c.copy(reg = x) }
        .text(s"reg is the regularization coefficient, default if ${default_params.reg}")
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
      note(
        """
          |For example, the following command runs this app on your data set:
          |
          | java -jar out/artifacts/*/*.jar \
          |   --learn_rate 0.1 \
          |   --reg 0.1 \
          |   --max_iter 10 \
          |   --min_info_gain 1e-6 \
          |   --train_pt ...
          |   --valid_pt ...
          |   --model_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val sparkConf = new SparkConf()
      .setAppName("Spark Logistic Regression Training")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(sparkConf)

    val n_partition = if (p.n_partition > 0)
      p.n_partition
    else
      sc.defaultMinPartitions

    val points = sc.textFile(p.train_pt, n_partition).map(LabeledPoint.parse).cache()

    // prepare training and validate datasets
    val (train_points, valid_points) = if (!p.validate_pt.isEmpty) {
      val points2 = sc.textFile(p.validate_pt, n_partition).map(LabeledPoint.parse).cache()
      (points, points2)
    } else if (p.split_ratio > 0.0 && p.split_ratio < 1) {
      // split points for training and test
      val Array(ps1, ps2) = points.randomSplit(Array(p.split_ratio, 1 - p.split_ratio))
      (ps1, ps2)
    } else {
      // train without validation
      (points, null)
    }

    val class_num = train_points.map { p =>
      math.round(p.label)
    }.distinct().collect().length

    val model: LRModel = LR.train(train_points, valid_points,
      class_num, p.graphx, p.max_iter, p.learn_rate, p.reg)

    if (!p.model_pt.isEmpty)
      model.save(p.model_pt)
  }

  /** command line parameters */
  case class Params(train_pt: String = "",
                    validate_pt: String = "",
                    model_pt: String = "",
                    graphx: Boolean = false,
                    n_partition: Int = -1,
                    split_ratio: Double = 1.0,
                    learn_rate: Double = 0.01,
                    reg: Double = 0.01,
                    max_iter: Int = 100)
}
