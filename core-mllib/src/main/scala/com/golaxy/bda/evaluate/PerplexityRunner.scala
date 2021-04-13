package com.golaxy.bda.evaluate

//import bda.spark.model.LDA.LDAModel
import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineStage, PipelineModel}
import org.apache.spark.ml.clustering.{LDAModel, LDA}
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import bda.common.Logging

import scala.collection.mutable.ArrayBuffer


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
      opt[String]("input_pt")
        .required()
        .text("input_pt")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("output_pt")
        .action((x, c) => c.copy(output_pt = x))
//      note(
//        """
//          |For example, the following command runs this app on your predictions:
//          |
//          | java -jar out/artifacts/*/*.jar \
//          |   --model_pt ... \
//          |   --doc_pt ...
//        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val spark = SparkSession.builder()
      .appName("PerplexityRunner")
//      .master("local")
      .getOrCreate()

    var df = DFUtils.loadcsv(spark,p.input_pt)

    val model = PipelineModel.load(p.model_pt)
    val dfPipe =  model.stages(0).transform(df)
//    dfPipe.show()

    val lp = model.stages(1).asInstanceOf[LDAModel].logPerplexity(dfPipe)
    val ll = model.stages(1).asInstanceOf[LDAModel].logLikelihood(dfPipe)

    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println("perplexity=" + lp)

    val evaluateDf = spark.createDataFrame(Seq(
      ("perplexity", lp),("likelihood",ll)
    )) toDF("Index", "value")
    evaluateDf.show()
    DFUtils.exportcsv(evaluateDf,p.output_pt)
    spark.stop()
  }

  /** command line parameters */
  case class Params(model_pt: String = "D:\\testdata\\runnable\\LDA\\output_model",
                    input_pt: String = "D:\\testdata\\evaluate\\PerplexityRunner\\input_pt\\win.csv",
                    output_pt: String = "D:\\testdata\\evaluate\\PerplexityRunner\\out_put"
                   )
}
