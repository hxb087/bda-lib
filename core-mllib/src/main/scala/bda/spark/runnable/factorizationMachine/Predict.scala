package bda.spark.runnable.factorizationMachine

import bda.common.obj.LabeledPoint
import bda.spark.model.factorizationMachine.FMModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object Predict {
  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("FM Predict") {
      head("FM Predict", "1.0")
      opt[String]("output_pt").required()
        .text("output_pt is the prediction output path.")
        .action { (x, c) => c.copy(output_pt = x) }
      opt[String]("test_pt").required()
        .text("test_pt is the test dataset path.")
        .action { (x, c) => c.copy(test_pt = x) }
      opt[String]("model_pt").required()
        .text("model_pt is a file to store the model parameters(weight).")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Int]("n_partition")
        .text(s"partition number, default is SparkContext.defaultMinPartitions.")
        .action { (x, c) => c.copy(n_partition = x) }
        .validate { x =>
          if (x >= 0) success else failure("Option --n_partition must >=0")
        }
      opt[Boolean]("graphx").required()
        .text(s"Whether use GraphX, default is ${default_params.graphx}")
        .action { (x, c) => c.copy(graphx = x) }
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
      .setAppName("FM Predict")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val test_points: RDD[LabeledPoint] = sc.textFile(para.test_pt, para.n_partition).map(LabeledPoint.parse).cache()
    //Load model.
    val fm_model = FMModel.load(sc, para.model_pt, para.graphx)

    test_points.map {
      pn =>
      val y = fm_model.predict(pn)
      s"${pn.id}\t${pn.label}\t$y"
    }.saveAsTextFile(para.output_pt)
    sc.stop()
  }

  /** command line parameters */
  case class Params(model_pt: String = "",
                    graphx: Boolean = false,
                    n_partition: Int = -1,
                    test_pt: String = "",
                    output_pt: String = ""
                   )
}

