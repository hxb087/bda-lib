package bda.spark.runnable.pagerank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Command line runner for PageRank
 * Input:
 * - model_pt format: i pr
 */
object Predict {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("PageRank Predict") {
      head("Predict", "1.0")
      opt[String]("model_pt").required()
        .text("model_pt is a file to store the pagerank value.")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[String]("name_pt")
        .text("name_pt file stores id and name pairs")
        .action { (x, c) => c.copy(name_pt = x) }
      opt[Int]("topK")
        .text(s"Output the top K users and their pagerank values, default is ${default_params.topK}.")
        .action { (x, c) => c.copy(topK = x) }
        .validate { x =>
        if (x > 0) success else failure("Option --topK must >0")
      }
      opt[String]("output_pt").required()
        .text("output_pt is a file to store the name -> pr result.")
        .action { (x, c) => c.copy(output_pt = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(p: Params) {
    val conf = new SparkConf()
      .setAppName("PageRank  Predict")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    val PR = bda.spark.model.pageRank.PagerankModel.load(sc, p.model_pt)

    val top = PR.getTopK(p.topK)
    sc.parallelize(top).map {
      case (id, pr) =>
        s"$id\t$pr"
    }.saveAsTextFile(p.output_pt)

    sc.stop
  }

  /** Parse the file into id name pairs. */
  private def parseName(sc: SparkContext,
                        name_pt: String): RDD[(VertexId, String)] = {
    sc.textFile(name_pt).map {
      line =>
        line.trim().split("\\s+") match {
          case Array(id, n) => (id.toLong, n)
          case other => throw new IllegalArgumentException(s"Bad format input: $other")
        }
    }
  }

  /** command line parameters */
  case class Params(model_pt: String = "",
                    name_pt: String = "",
                    output_pt: String = "",
                    topK: Int = 20)
}
