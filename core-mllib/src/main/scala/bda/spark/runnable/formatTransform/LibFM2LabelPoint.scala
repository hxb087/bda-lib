package bda.spark.runnable.formatTransform

import bda.spark.reader.Points
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object LibFM2LabeledPoint {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("LibFM2LabeledPoint") {
      head("LibFM2LabeledPoint", "1.0")
      opt[String]("input_pt").required()
        .text("Input libfm file")
        .action { (x, c) => c.copy(input_pt = x) }
      opt[String]("output_pt").required()
        .text("Output raw point file")
        .action { (x, c) => c.copy(output_pt = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val spark = SparkSession
      .builder()
      .appName("LibFM2LabeledPoint")
//      .master("local")
      .getOrCreate()


    val data = Points.readLibFMFile(spark, p.input_pt)
    data.saveAsTextFile(p.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "")

}
