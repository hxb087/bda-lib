package bda.spark.runnable.preprocess

import bda.common.obj.RawDoc
import bda.spark.preprocess.WordIndex
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
/**
  * Command Line runner of TFIDF computing
  */
object WordIndexTransformer {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("WordIndexTransform") {
      head("WordIndexTransform: Index words in documents with existing dictionary.")
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
      .setAppName(s"Spark TFIDF Transform")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(p.doc_pt).map(RawDoc.parse)

    val w2id = sc.textFile(p.dict_pt).map { ln =>
      val Array(w, id) = ln.split("\t")
      (w, id.toInt)
    }.collect().toMap

    val ds = WordIndex(docs, w2id)
    ds.saveAsTextFile(p.res_pt)
  }

  /** command line parameters */
  case class Params(doc_pt: String = "",
                    dict_pt: String = "",
                    res_pt: String = "")
}