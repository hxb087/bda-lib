package bda.spark.runnable.formatTransform

import bda.common.Logging
import bda.common.util.{Msg, Timer}
import bda.spark.reader.Points
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object LibSVM2LabeledPoint extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("LibSVM2LabeledPoint") {
      head("LibSVM2LabeledPoint", "1.0")
      opt[String]("input_pt").required()
        .text("Input libsvm file")
        .action { (x, c) => c.copy(input_pt = x) }
      opt[String]("output_pt").required()
        .text("Output raw point file")
        .action { (x, c) => c.copy(output_pt = x) }
      opt[Boolean]("is_class").required()
        .text("Whether the file is used for classification")
        .action { (x, c) => c.copy(is_class = x) }
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
      .appName("LibSVM2LabeledPoint")
//      .master("local")
      .getOrCreate()

    val data = Points.readLibSVMFile(spark, p.input_pt, p.is_class)

    val timer = new Timer
    data.saveAsTextFile(p.output_pt)
    val t = timer.cost()
    val msg = Msg("TimeOfDumping2HDFS" -> s"${t}ms")
    logInfo(msg.toString)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    is_class: Boolean = true)

}
