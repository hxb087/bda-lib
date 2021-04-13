package bda.spark.runnable.preprocess

import bda.common.obj.RawPoint
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.{FeatureMerge => FM}
import org.apache.log4j.{Level, Logger}

/**
 * Preprocess for FeatureMerge.
 */
object FeatureMergeRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkPreprocessFeatureMerge") {
      head("RunSparkPreprocessFeatureMerge: an example app of preprocessing for FeatureMerge.")
      opt[String]("input_pt1")
        .required()
        .text("one of directories of the input records")
        .action((x, c) => c.copy(input_pt1 = x))
      opt[String]("input_pt2")
        .required()
        .text("another directory of the output indexed records")
        .action((x, c) => c.copy(input_pt2 = x))
      opt[String]("output_pt")
        .required()
        .text("directory of the output")
        .action((x, c) => c.copy(output_pt = x))
      note(
        """
          |For example, the following command runs this app to preprocess for FeatureMerge:
          |
          | bin/spark-submit --class bda.runnable.preprocess.FeatureMerge \
          |   out/artifacts/*/*.jar \
          |   --input_pt1 ... \
          |   --input_pt2 ... \
          |   --output_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of FeatureMerge")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val rds1 = sc.textFile(params.input_pt1).map { ln =>
      RawPoint.parse(ln)
    }
    val rds2 = sc.textFile(params.input_pt2).map { ln =>
      RawPoint.parse(ln)
    }

    val rds = FM(rds1, rds2)
    rds.map(_.toString).saveAsTextFile(params.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt1: String = "",
                    input_pt2: String = "",
                    output_pt: String = "")
}