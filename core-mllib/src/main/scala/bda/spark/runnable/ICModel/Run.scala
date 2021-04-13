package bda.spark.runnable.ICModel

import bda.spark.model.ICModel.ICModel
import bda.spark.reader.GraphLoader
import bda.common.Logging
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

/**
  * Command line runner for ICModel.
  * Input:
  * --edge_pt format:i j (v) --partition Int -- max_iter Int --threshold Double
  * --seed String format "a b c d e"
  */
object Run extends Logging {

//<<<<<<< HEAD
//  /** command line parameters */
//  case class Params(edge_pt: String = "",
//                    max_iter: Int = 200,
//                    n_partition: Int = 0,
//                    threshold: Double = 0.8,
//                    seed: String = "0")
//
//=======
//>>>>>>> 6604989a5cd85d29aa0f4cf6e94f94f027065946
  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("ICModel") {
      head("ICModel", "1.0")
      opt[String]("edge_pt").required()
        .text("edge_pt is an edge file which stores a graph.")
        .action { (x, c) => c.copy(edge_pt = x) }
      opt[Int]("n_partition")
        .text(s"the min number of RDD's split parts, default is SparkContext.defaultMinPartitions.")
        .action { (x, c) => c.copy(n_partition = x) }
        .validate { x =>
          if (x >= -1) success else failure("Option --partition must >0")
        }
      opt[Int]("max_iter")
        .text(s"Number of iterations, default is ${default_params.max_iter}.")
        .action { (x, c) => c.copy(max_iter = x) }
        .validate { x =>
          if (x >= 0) success else failure("Option --max_iter must >=0")
        }
      opt[String]("seed").required()
        .text("seed is the original active vertices.")
        .action { (x, c) => c.copy(seed = x) }
      opt[Double]("threshold").required()
        .text("threshold is the threshold it's activated.")
        .action { (x, c) => c.copy(threshold = x) }
        .validate { x =>
          if (x >= 0.0) success else failure("Option --threshold must >0.0")
        }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(p: Params) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("ICModel")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/tmp")

    val seed = p.seed.split("\\s+").map { s =>
      s.toLong
    }.toSet
    val n_partition = if (p.n_partition > 0) p.n_partition else sc.defaultMinPartitions

    val edges: RDD[(Long, Long)] = sc.textFile(p.edge_pt, n_partition).map {
      line =>
        val Array(src, dst) = line.split("\\s+")
        (src.toLong, dst.toLong)
    }

    val g: Graph[Int, Int] = GraphLoader.loadGraph_AB(edges, 0, 0)

    val max_activated: Double = ICModel.run(g, seed, p.max_iter, p.threshold)
    logInfo(s"The activated vertices count is $max_activated.")
  }

  /** command line parameters */
  case class Params(edge_pt: String = "",
                    max_iter: Int = 200,
                    n_partition: Int = -1,
                    threshold: Double = 0.8,
                    seed: String = "0")
}
