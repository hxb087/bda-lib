package bda.spark.runnable.preprocess

import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.DocWordSeg
import org.apache.log4j.{Level, Logger}

/**
  * Preprocess for DocsTermCount.
  */
object DocWordSegRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkDocWordSeg") {
      head("RunSparkDocWordSeg: an example app for word segement of documents.")
      opt[String]("input_pt")
        .required()
        .text("directory of the input data set")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("directory of the output data set")
        .action((x, c) => c.copy(output_pt = x))
      opt[Boolean]("tagged")
        .text("Whether have tags of the words")
        .action((x, c) => c.copy(tagged = x))
      opt[Boolean]("has_id")
        .text("Whether provided document ID")
        .action((x, c) => c.copy(has_id = x))
      opt[Boolean]("has_label")
        .text("Whether provided document Label")
        .action((x, c) => c.copy(has_label = x))
      opt[String]("seperator")
        .text("string used to seperate fileds")
        .action((x, c) => c.copy(seperator = x))
      note(
        """
          |For example, the following command runs this app to preprocess for WordSeg:
          |
          | bin/spark-submit --class bda.runnable.preprocess.WordSeg \
          |   out/artifacts/*/*.jar \
          |   --input_pt ... \
          |   --output_pt ... \
          |   --has_id false \
          |   --has_label false \
          |   --seperator \t
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of WordSeg")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(params.input_pt)

    val segs = DocWordSeg(docs, params.tagged, params.has_id, params.has_label, params.seperator)
    segs.map(_.toString).saveAsTextFile(params.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    tagged: Boolean = false,
                    has_id: Boolean = true,
                    has_label: Boolean = true,
                    seperator: String = "\t")
}