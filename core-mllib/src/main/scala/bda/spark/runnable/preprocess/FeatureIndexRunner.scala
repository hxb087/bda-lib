package bda.spark.runnable.preprocess

import bda.common.obj.RawPoint
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.FeatureIndex
import org.apache.log4j.{Level, Logger}

/**
 * Preprocess for FeatureIndex.
 */
object FeatureIndexRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("FeatureIndexRunner") {
      head("FeatureIndexRunner")
      opt[String]("input_pt")
        .required()
        .text("directory of the input records")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("directory of the output indexed records")
        .action((x, c) => c.copy(output_pt = x))
      opt[String]("dict_pt")
        .required()
        .text("directory of the output dictionary")
        .action((x, c) => c.copy(dict_pt = x))
      note(
        """
          |For example, the following command runs this app to preprocess for FeatureIndex:
          |
          | bin/spark-submit --class bda.runnable.preprocess.FeatureIndex \
          |   out/artifacts/*/*.jar \
          |   --input_pt ... \
          |   --output_pt ... \
          |   --dict_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of FeatureIndex")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val rds = sc.textFile(params.input_pt).map { ln =>
      RawPoint.parse(ln)
    }

    val (indexed_rds, dict) = FeatureIndex(rds)

    println("n(feature)=" + dict.size)
    indexed_rds.map(_.toString).saveAsTextFile(params.output_pt)
    sc.parallelize(dict.toSeq).map{case (f, fid) => s"$f\t$fid"}
      .saveAsTextFile(params.dict_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    dict_pt: String = "")
}