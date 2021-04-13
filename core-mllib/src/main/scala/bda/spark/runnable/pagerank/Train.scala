package bda.spark.runnable.pagerank

import bda.common.util.DFUtils
import bda.spark.reader.GraphLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Command line runner for PageRank
 * Input:
 * --edge_pt format:i j --model_pt format:i pr --name_pt format:i name
 *
 */
object Train {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("PageRank Train") {
      head("Train", "1.0")
      opt[String]("model_pt").required()
        .action { (x, c) => c.copy(model_pt = x) }
        .text("file to store the pagerank model.")
      opt[String]("edge_pt").required()
        .action { (x, c) => c.copy(edge_pt = x) }
        .text("Edge file of the input graph.")
      opt[Int]("max_iter")
        .text(s"Number of iterations, default is ${default_params.max_iter}")
        .action { (x, c) => c.copy(max_iter = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --max_iter must >0")
        }
      opt[Double]("damping_factor")
        .text(s"Probability of diverting to linked pages, default is ${default_params.damping_factor}.")
        .action { (x, c) => c.copy(damping_factor = x) }
        .validate { x =>
          if (x >= 0.0) success else failure("Option --damping_factor must >0.")
        }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(p: Params) {
    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("PageRank Train")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setCheckpointDir("/tmp")

    val rawDF = DFUtils.loadcsv(spark,p.edge_pt)
    val edges: RDD[(Long, Long)] = rawDF.rdd.map {
      row =>
        (row.get(0).toString.toLong, row.get(1).toString.toLong)
    }

    val g = GraphLoader.loadGraph_AB(edges, 0.0, 1.0, dir = false).cache()
    val PR = bda.spark.model.pageRank.PageRank.run(g, p.max_iter, p.damping_factor)
    PR.saveModel(p.model_pt)
  }

  /** command line parameters */
  case class Params(edge_pt: String = "",
                    model_pt: String = "",
                    damping_factor: Double = 0.85,
                    max_iter: Int = 15)

}
