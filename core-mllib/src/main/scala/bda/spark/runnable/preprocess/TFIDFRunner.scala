package bda.spark.runnable.preprocess

import bda.common.obj.RawDoc
import bda.spark.preprocess.TFIDF
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

object TFIDFRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("TFIDF") {
      head("TFIDF: Compute the tfidf for a document collection..")
      opt[String]("doc_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(doc_pt = x))
      opt[String]("idf_pt")
        .required()
        .text("File Stores words' idf values")
        .action((x, c) => c.copy(idf_pt = x))
      opt[String]("res_pt")
        .required()
        .text("File Stores documents with tfidf representation")
        .action((x, c) => c.copy(res_pt = x))
      note(
        """
          |For example, the following command runs this app on your cluster:
          |
          | bin/spark-submit --class bda.runnable.preprocess.TFIDFRunner \
          |   out/artifacts/*/*.jar \
          |   --doc_pt ...  \
          |   --idf_pt ...  \
          |   --res_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of TFIDF")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val docs = sc.textFile(params.doc_pt).map(RawDoc.parse)
    val (ds, idf) = TFIDF(docs)

    sc.makeRDD(idf.toSeq).map {
      case (w, v) => s"$w\t$v"
    }.saveAsTextFile(params.idf_pt)

    ds.saveAsTextFile(params.res_pt)
  }

  /** command line parameters */
  case class Params(doc_pt: String = "",
                    idf_pt: String = "",
                    res_pt: String = "")
}
