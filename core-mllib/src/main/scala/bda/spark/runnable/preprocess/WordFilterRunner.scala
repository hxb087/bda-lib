package bda.spark.runnable.preprocess

import bda.common.obj.RawDoc
import bda.spark.preprocess.WordFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Command Line runner of word filtering
  */
object WordFilterRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("WordFilterRuner") {
      head("WordFilterRuner: Filter noise words in documents.")
      opt[String]("doc_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(doc_pt = x))
      opt[String]("stopWords_pt")
        .text("stop word list")
        .action((x, c) => c.copy(stopWords_pt = x))
      opt[Int]("min_freq")
        .text(s"filter words with frequency less than min_freq, default is ${default_params.min_freq}")
        .action((x, c) => c.copy(min_freq = x))
      opt[Int]("max_freq")
        .text(s"filter words with frequency less than max_freq, default is ${default_params.max_freq}")
        .action((x, c) => c.copy(max_freq = x))
      opt[Int]("topN")
        .text(s"filter the topN most frequent words, default is ${default_params.topN}")
        .action((x, c) => c.copy(topN = x))
      opt[Int]("min_len")
        .text(s"filter the words with length less than min_len, default is ${default_params.min_len}")
        .action((x, c) => c.copy(min_len = x))
      opt[Int]("max_len")
        .text(s"filter the words with length more than max_len, default is ${default_params.max_len}")
        .action((x, c) => c.copy(max_len = x))
      opt[String]("res_pt")
        .required()
        .text("File Stores documents with word index representation")
        .action((x, c) => c.copy(res_pt = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of Word Filter")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(p.doc_pt).map(RawDoc.parse)

    val stop_words = if (p.stopWords_pt.isEmpty) Set.empty[String]
    else sc.textFile(p.stopWords_pt).map(_.trim).collect().toSet

    val ds: RDD[RawDoc] = WordFilter(docs, stop_words, p.min_freq,
      p.max_freq, p.topN, p.min_len, p.max_len)

    ds.saveAsTextFile(p.res_pt)
  }

  /** command line parameters */
  case class Params(doc_pt: String = "",
                    stopWords_pt: String = "",
                    min_freq: Int = 5,
                    max_freq: Int = Int.MaxValue,
                    topN: Int = 100,
                    min_len: Int = 2,
                    max_len: Int = 10,
                    res_pt: String = "")
}