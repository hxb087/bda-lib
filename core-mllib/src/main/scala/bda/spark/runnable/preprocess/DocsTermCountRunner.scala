package bda.spark.runnable.preprocess

import bda.common.obj.RawDoc
import bda.common.util.io.{readLines, writeMap}
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.preprocess.{DocsTermCount => DTC}
import org.apache.log4j.{Level, Logger}

/**
 * Preprocess for DocsTermCount.
 */
object DocsTermCountRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkPreprocessDocsTermCount") {
      head("RunSparkPreprocessDocsTermCount: an example app of preprocessing for DocsTermCount.")
      opt[String]("input_pt")
        .required()
        .text("directory of the input data set")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("directory of the output data set")
        .action((x, c) => c.copy(output_pt = x))
      opt[String]("term_type")
        .text("\"unigram\" or \"bigram\" or \"biterm\"")
        .action((x, c) => c.copy(term_type = x))
      opt[Boolean]("is_doc_freq")
        .text("If true, count the document frequency of words; Else count the overall term frequency of words.")
        .action((x, c) => c.copy(is_doc_freq = x))
      note(
        """
          |For example, the following command runs this app to preprocess for DocsTermCount:
          |
          | bin/spark-submit --class bda.runnable.preprocess.DocsTermCount \
          |   out/artifacts/*/*.jar \
          |   --output_pt ... \
          |   --term_type unigram \
          |   --is_doc_freq false
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of DocsTermCount")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(params.input_pt).map { ln =>
      RawDoc.parse(ln)
    }

    val tf = DTC(docs, params.term_type, params.is_doc_freq)
    sc.parallelize(tf.toSeq).map{case (w, n) => s"$w\t$n"}.saveAsTextFile(params.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    term_type: String = "unigram",
                    is_doc_freq: Boolean = false)
}