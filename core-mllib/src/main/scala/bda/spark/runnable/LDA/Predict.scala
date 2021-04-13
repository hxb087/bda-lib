package bda.spark.runnable.LDA

import bda.common.Logging
import bda.common.obj.Doc
import bda.common.util.Msg
import bda.spark.evaluate.TopicModel.perplexity
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.model.LDA.LDAModel
import org.apache.log4j.{Level, Logger}


/**
  * Command line wrapper for Spark LDA Predict, i.e.,
  * predict P(z|d) for documents.
  */
object Predict extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Spark LDA Predict") {
      head("Predict", "1.0")
      opt[String]("test_pt").required()
        .text("The path of document file. Each line is a document, represented by word index sequences, e.g., \"wid wid ...\"")
        .action { (x, c) => c.copy(test_pt = x) }
      opt[String]("model_pt").required()
        .text("The path to write the model (word distribution of the topics).")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[String]("pz_d_pt").required()
        .text("The path to write the P(z|d) of each document.")
        .action { (x, c) => c.copy(pz_d_pt = x) }
      opt[Boolean]("graphx")
        .text("If true, using the GraphX implementation; Else use the shared model implementation. Default is false.")
        .action { (x, c) => c.copy(graphx = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Spark LDA Prediction")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(sparkConf)

    val model = LDAModel.load(sc, params.model_pt, params.graphx)
    val docs = sc.textFile(params.test_pt).map(Doc.parse).cache()

    // compute the perplexity of test documents
    val score = perplexity(model, docs)
    val msg = Msg("n(docs)" -> docs.count(), "perplexity" -> score)
    logInfo(msg.toString)

    // output the inference of the test documents
    if (params.pz_d_pt.nonEmpty) {
      val pz_d = model.predict(docs)
      logInfo("save P(z|d):" + params.pz_d_pt)
      pz_d.saveAsTextFile(params.pz_d_pt)
    }
  }

  /** command line parameters */
  case class Params(test_pt: String = "",
                    model_pt: String = "",
                    pz_d_pt: String = "",
                    graphx: Boolean = false)

}
