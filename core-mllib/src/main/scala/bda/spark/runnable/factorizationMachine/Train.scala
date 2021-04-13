package bda.spark.runnable.factorizationMachine

import bda.common.obj.LabeledPoint
import org.apache.log4j.{Level, Logger}
import bda.spark.model.factorizationMachine.FM
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Train {

  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("FM Train") {
      head("FM Train", "1.0")
      opt[Boolean]("is_regression")
        .text("Regression or classification.")
        .action { (x, c) => c.copy(is_regression = x) }
      opt[String]("train_pt").required()
        .text("train_pt is the train dataset path.")
        .action { (x, c) => c.copy(train_pt = x) }
      opt[String]("validate_pt")
        .text("validate_pt is the validate dataset path.")
        .action { (x, c) => c.copy(validate_pt = x) }
      opt[String]("model_pt").required()
        .text("model_pt is a file to store the model parameters(weight).")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Boolean]("k1")
        .text(s"k1 is whether you need the 1 D weight, default is ${default_params.k1}.")
        .action { (x, c) => c.copy(k1 = x) }
      opt[Boolean]("graphx")
        .text(s"Whether use GraphX, default is ${default_params.graphx}")
        .action { (x, c) => c.copy(graphx = x) }
      opt[Int]("k2")
        .text(s"k2 is the dimension of 2 D interaction, default is ${default_params.k2}.")
        .action { (x, c) => c.copy(k2 = x) }
        .validate { x =>
        if (x > 0) success else failure("Option --k2 should be bigger than 0")
      }
      opt[Int]("max_iter")
        .text(s"the max of iterations, default is ${default_params.max_iter}")
        .action { (x, c) => c.copy(max_iter = x) }
        .validate { x =>
        if (x > 0) success else failure("Option --maxIteration must >0")
      }
      opt[Int]("n_partition")
        .text(s"the min number of RDD's split parts, default is SparkContext.defaultMinPartitions.")
        .action { (x, c) => c.copy(n_partition = x) }
        .validate { x =>
        if (x >= 0) success else failure("Option --n_partition must >= 0")
      }
      opt[Double]("reg1")
        .text(s"the regularization of 1 way interaction, default is ${default_params.reg1}")
        .action { (x, c) => c.copy(reg1 = x) }
        .validate { x =>
        if (x >= 0.0) success else failure("Option --reg must >0.0")
      }
      opt[Double]("reg2")
        .text(s"the regularization of 2 way interaction, ${default_params.reg2}")
        .action { (x, c) => c.copy(reg2 = x) }
        .validate { x =>
        if (x >= 0.0) success else failure("Option --reg must >0.0")
      }
      opt[Double]("learn_rate")
        .text(s"learn_rate, default is ${default_params.learn_rate}")
        .action { (x, c) => c.copy(learn_rate = x) }
        .validate { x =>
        if (x > 0.00) success else failure("Option --learnRate must >0.00")
      }
      opt[Double]("split_ratio")
        .text(s"split_ratio, default is ${default_params.split_ratio}")
        .action { (x, c) => c.copy(split_ratio = x) }
        .validate { x =>
        if (x > 0.00 && x < 1.0) success else failure("Option --split_ratio must >0.00 and  < 1.0")
      }
      opt[Double]("stdev")
        .text(s"stdev, default is ${default_params.stdev}")
        .action { (x, c) => c.copy(stdev = x) }
        .validate { x => if (x > 0.00) success else failure("Option --stdev must >0.00") }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(para: Params): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("FM Train")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val dim = (para.k1, para.k2)
    val reg = (para.reg1, para.reg2)
    val points: RDD[LabeledPoint] = sc.textFile(para.train_pt, para.n_partition).map(LabeledPoint.parse).cache()

    // prepare training and validate datasets
    val (train_points, valid_points) = if (!para.validate_pt.isEmpty) {
      val points2 = sc.textFile(para.validate_pt, para.n_partition).map(LabeledPoint.parse).cache()
      (points, points2)
    } else if (para.split_ratio > 0.0 && para.split_ratio < 1) {
      // split points for training and test
      val Array(ps1, ps2) = points.randomSplit(Array(para.split_ratio, 1 - para.split_ratio))
      (ps1.cache(), ps2.cache())
    } else {
      // train without validation
      (points.cache(), null)
    }

    //Train FM model.
    val trainer = FM.train(train_points, valid_points,
      para.is_regression, para.learn_rate, para.max_iter,
      para.graphx, dim, reg, para.stdev)
    trainer.saveModel(sc, para.model_pt)
    sc.stop()
  }

  /** command line parameters */
  case class Params(is_regression: Boolean = true,
                    train_pt: String = "",
                    validate_pt: String = "",
                    model_pt: String = "",
                    graphx: Boolean = false,
                    n_partition: Int = -1,
                    learn_rate: Double = 0.001,
                    k1: Boolean = true,
                    k2: Int = 8,
                    reg1: Double = 0.001,
                    reg2: Double = 0.001,
                    stdev: Double = 0.001,
                    split_ratio: Double = 0.8,
                    max_iter: Int = 50)

}

