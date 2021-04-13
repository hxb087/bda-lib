package bda.spark.runnable.preprocess

import bda.common.obj.RawPoint
import bda.common.util.io.{readLines, readMap, writeLines}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.{MinMaxScale => MMS}
import org.apache.log4j.{Level, Logger}

/**
 * Preprocess for MinMaxScaleTransform.
 */
object MinMaxScaleTransformer {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkPreprocessMinMaxScaleTransform") {
      head("RunSparkPreprocessMinMaxScaleTransform: an example app of preprocessing for MinMaxScale.")
      opt[String]("input_data_pt")
        .required()
        .text("directory of the input data")
        .action((x, c) => c.copy(input_data_pt = x))
      opt[String]("output_data_pt")
        .required()
        .text("directory of the output data")
        .action((x, c) => c.copy(output_data_pt = x))
      opt[String]("input_mins_maxs_pt")
        .required()
        .text("directory of the minimum and maximum values of each feature for input")
        .action((x, c) => c.copy(input_mins_maxs_pt = x))
      note(
        """
          |For example, the following command runs this app to preprocess for MinMaxScale:
          |
          | bin/spark-submit --class bda.runnable.preprocess.MinMaxScaleFitTransform \
          |   out/artifacts/*/*.jar \
          |   --input_mins_pt ... \
          |   --input_maxs_pt ... \
          |   --output_data_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of MinMaxScale")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val input_data = sc.textFile(params.input_data_pt).map { ln =>
      RawPoint.parse(ln)
    }
    val mins_maxs = sc.textFile(params.input_mins_maxs_pt).map { ln =>
      val Array(f, min_max) = ln.trim().split("\t")
      val Array(min, max) = min_max.split(",")
      (f, (min.toDouble, max.toDouble))
    }.collectAsMap().toMap
    val mins = mins_maxs.map(e => (e._1, e._2._1))
    val maxs = mins_maxs.map(e => (e._1, e._2._2))

    val output_data = MMS(input_data, mins, maxs)
    output_data.map(_.toString).saveAsTextFile(params.output_data_pt)
  }

  /** command line parameters */
  case class Params(input_data_pt: String = "",
                    input_mins_maxs_pt: String = "",
                    output_data_pt: String = "")
}