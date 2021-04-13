package bda.spark.runnable.preprocess

import bda.common.Logging
import bda.common.util.{Msg, Timer}
import com.golaxy.bda.utils.DFUtils
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
  * Preprocess for FeatureMerge.
  */
object FileSplitRunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("FileSplit") {
      head("FileSplit")
      opt[String]("input_pt")
        .required()
        .text("the directory of the input file")
        .action((x, c) => c.copy(input_pt = x))
      opt[Double]("ratio")
        .required()
        .text("the ratio to split the file")
        .action((x, c) => c.copy(ratio = x))
      opt[String]("output_pt1")
        .required()
        .text("the directory of one of output files")
        .action((x, c) => c.copy(output_pt1 = x))
      opt[String]("output_pt2")
        .required()
        .text("the directory of another output file")
        .action((x, c) => c.copy(output_pt2 = x))
      opt[Boolean]("isOrNotHeader")
        .required()
        .text("file is or not have header")
        .action((x, c) => c.copy(isOrNotHeader = x))
      note(
        """
          |For example, the following command runs this app to preprocess for FileSplit:
          |
          | java -jar out/artifacts/*/*.jar \
          |   --input_pt ... \
          |   --ratio 0.5 \
          |   --output_pt1 ... \
          |   --output_pt2 ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    var df: DataFrame = null
    var f: RDD[String] = null
    var t1: Long = 0
    var t2: Long = 0
    if (params.isOrNotHeader) {
      val spark = SparkSession.builder().getOrCreate()
      df = DFUtils.loadcsv(spark, params.input_pt)
      val ratios = Array(params.ratio, 1.0 - params.ratio)
      val Array(f1, f2) = df.randomSplit(ratios, Random.nextLong())

      f1.count()
      val timer1 = new Timer
      DFUtils.exportcsv(f1, params.output_pt1)
      t1 = timer1.cost()

      f2.count()
      val timer2 = new Timer
      DFUtils.exportcsv(f2, params.output_pt2)
      t2 = timer2.cost()
    } else {
      val conf = new SparkConf()
        .setAppName(s"Spark Preprocess of FileSplit")
        .set("spark.hadoop.validateOutputSpecs", "false")
      val sc = new SparkContext(conf)

      val f = sc.textFile(params.input_pt)
      val ratios = Array(params.ratio, 1.0 - params.ratio)
      val Array(f1, f2) = f.randomSplit(ratios, Random.nextLong())

      f1.count()
      val timer1 = new Timer
      f1.saveAsTextFile(params.output_pt1)
      t1 = timer1.cost()

      f2.count()
      val timer2 = new Timer
      f2.saveAsTextFile(params.output_pt2)
      t2 = timer2.cost()
    }


    val msg = Msg("TimeOfDumping2HDFS(output_1)" -> s"${t1}ms",
      "TimeOfDumping2HDFS(output_2)" -> s"${t2}ms")
    logInfo(msg.toString)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    ratio: Double = 0.5,
                    output_pt1: String = "",
                    output_pt2: String = "",
                    isOrNotHeader: Boolean = false
                   )

}