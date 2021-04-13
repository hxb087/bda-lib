package bda.spark.runnable.preprocess

import bda.common.obj.RawPoint
import bda.common.util.io.{readLines, writeMap, writeLines}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.{MinMaxScale => MMS}
import org.apache.log4j.{Level, Logger}

/**
 * Preprocess for MinMaxScale.
 */
object MinMaxScaleRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkPreprocessMinMaxScale") {
      head("RunSparkPreprocessMinMaxScale: an example app of preprocessing for MinMaxScale.")
      opt[String]("input_data_pt")
        .required()
        .text("directory of the input data")
        .action((x, c) => c.copy(input_data_pt = x))
      opt[String]("output_data_pt")
        .required()
        .text("directory of the output data")
        .action((x, c) => c.copy(output_data_pt = x))
      opt[String]("output_mins_maxs_pt")
        .required()
        .text("directory of the minimum and maxmum values of each feature for output")
        .action((x, c) => c.copy(output_mins_maxs_pt = x))
      note(
        """
          |For example, the following command runs this app to preprocess for MinMaxScale:
          |
          | bin/spark-submit --class bda.runnable.preprocess.MinMaxScaleFit \
          |   out/artifacts/*/*.jar \
          |   --input_data_pt ... \
          |   --output_data_pt ... \
          |   --output_mins_maxs_pt ...
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

    val (output_data, mins, maxs) = MMS(input_data)
    output_data.map(_.toString).saveAsTextFile(params.output_data_pt)

    require(mins.size == maxs.size)
    val mins_maxs = mins.map(e => (e._1, (e._2, maxs(e._1))))
    sc.parallelize(mins_maxs.toSeq).map { case (f, (min, max)) =>
        s"$f\t$min,$max"
    }.saveAsTextFile(params.output_mins_maxs_pt)
  }

  /** command line parameters */
  case class Params(input_data_pt: String = "",
                    output_data_pt: String = "",
                    output_mins_maxs_pt: String = "")
}