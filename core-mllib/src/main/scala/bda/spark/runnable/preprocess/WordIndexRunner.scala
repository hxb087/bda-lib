package bda.spark.runnable.preprocess

import bda.common.obj.RawDoc
import bda.spark.preprocess.WordIndex
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
  * Command Line runner of TFIDF computing
  */
object WordIndexRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("WordIndexFit") {
      head("WordIndexFit: Index words in documents.")
      opt[String]("doc_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(doc_pt = x))
      opt[String]("dict_pt")
        .required()
        .text("File Stores words' index")
        .action((x, c) => c.copy(dict_pt = x))
      opt[String]("res_pt")
        .required()
        .text("File Stores documents with word index representation")
        .action((x, c) => c.copy(res_pt = x))
      note(
        """
          |For example, the following command runs this app on your predictions:
          |
          | java -jar local.jar \
          |   --doc_pt ...
          |   --dict_pt ...
          |   --res_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of Word Index")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(p.doc_pt).map(RawDoc.parse)

    val (ds, w2id) = WordIndex(docs)
    ds.saveAsTextFile(p.res_pt)

    println("n(word)=" + w2id.size)
    // write word-index map
    sc.makeRDD(w2id.toSeq).map {
      case (w, i) => s"$w\t$i"
    }.saveAsTextFile(p.dict_pt)
  }

  /** command line parameters */
  case class Params(doc_pt: String = "",
                    dict_pt: String = "",
                    res_pt: String = "")
}