package bda.spark.runnable.evaluate

import bda.spark.evaluate.TopicModel
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.common.obj.Doc
import bda.spark.model.LDA.LDAModel
import org.apache.log4j.{Level, Logger}
import bda.common.Logging

/**
  * Evaluate for a spark LDA model
  */
object PerplexityRunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("TopicModelEvaluate") {
      head("TopicModelEvaluate: LDA model evaluation on Spark.")
      opt[String]("model_pt")
        .required()
        .text("directory of the model")
        .action((x, c) => c.copy(model_pt = x))
      opt[String]("doc_pt")
        .required()
        .text("Test documents")
        .action((x, c) => c.copy(doc_pt = x))
      opt[Boolean]("graphx")
        .required()
        .text("Whether a GraphX LDAModel or Not")
        .action((x, c) => c.copy(graphx = x))
      note(
        """
          |For example, the following command runs this app on your predictions:
          |
          | java -jar out/artifacts/*/*.jar \
          |   --model_pt ... \
          |   --doc_pt ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Spark Evaluation of LDA model")
    val sc = new SparkContext(conf)

    val model = LDAModel.load(sc, params.model_pt, params.graphx)
    val docs = sc.textFile(params.doc_pt).map {
      Doc.parse
    }

    val perplexity = TopicModel.perplexity(model, docs)

    println("perplexity=" + perplexity)
  }

  /** command line parameters */
  case class Params(model_pt: String = "",
                    doc_pt: String = "",
                    graphx: Boolean = false)
}
