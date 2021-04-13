package bda.spark.runnable.formatTransform

import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import bda.spark.reader.SVDFeaturePoints
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Rate2SVDFeaturePoint {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Rate2SVDFeaturePoint") {
      head("Rate2SVDFeaturePoint", "1.0")
      opt[String]("rates_pt").required()
        .text("Input rate file")
        .action { (x, c) => c.copy(rates_pt = x) }
      opt[String]("points_pt").required()
        .text("The point file")
        .action { (x, c) => c.copy(points_pt = x) }
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
      .appName("Rate2SVDFeaturePoint")
      .config("spark.hadoop.validateOutputSpecs", "false")
//      .master("local")
      .getOrCreate()

    val points = SVDFeaturePoints.readRatesFile(spark, p.rates_pt)

    points.saveAsTextFile(p.points_pt)
  }

  /** command line parameters */
  case class Params(rates_pt: String = "",
                    points_pt: String = "")

}
