package bda.spark.runnable.formatTransform


import bda.common.obj.LabeledPoint
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object LabelTransform {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("LabelTransform") {
      head("LabelTransform", "1.0")
      opt[String]("input_pt").required()
        .text("Input libsvm format labelpoint file with label 0")
        .action { (x, c) => c.copy(input_pt = x) }
      opt[String]("output_pt").required()
        .text("Output libfm format labelpoint file with label -1")
        .action { (x, c) => c.copy(output_pt = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val conf = new SparkConf()
      .setAppName("LabelPoint Transform")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val input_data: RDD[LabeledPoint] = sc.textFile(p.input_pt).map(LabeledPoint.parse).map{
      case la =>
        if (la.label == 0)
          LabeledPoint(la.id, la.label, la.fs)
        else
          la
    }

    input_data.saveAsTextFile(p.output_pt)
  }
}

/** command line parameters */
case class Params(input_pt: String = "",
                  output_pt: String = "")
