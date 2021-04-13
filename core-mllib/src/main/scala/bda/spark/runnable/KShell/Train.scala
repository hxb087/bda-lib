package bda.spark.runnable.KShell

import bda.spark.model.KShell.KShell
import bda.spark.reader.GraphLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Train {
  /** Parse the command line parameters */
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("Kshell") {
      head("KShell Train", "1.0")
      opt[String]("edge_pt").required()
        .text("edge_pt is an edge file which stores a graph.")
        .action { (x, c) => c.copy(edge_pt = x) }
      opt[String]("model_pt").required()
        .text("model_pt is a file to store KShell values.")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[Int]("n_partition")
        .text(s"the min number of RDD's split parts, default is SparkContext.defaultMinPartitions.")
        .action { (x, c) => c.copy(n_partition = x) }
        .validate { x =>
        if (x >= 0) success else failure("Option --partition must >=0")
      }
      opt[String]("dir")
        .text(s"dir is how the graph degrees are generated, default is ${default_params.dir}.")
        .action { (x, c) => c.copy(dir = x) }
        .validate { x =>
        if (x == "degree" || x == "indegree") success else failure("Option --dir must be degree or indegree")
      }
      opt[Boolean]("weight")
        .text(s"weight is whether the degree is weight associated,default is ${default_params.weight}.")
        .action { (x, c) => c.copy(weight = x)
      }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(para: Params): Unit = {
    val conf = new SparkConf()
      .setAppName("KShell Trainer")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/tmp")

    val dir = para.dir
    val weight: Boolean = para.weight
    val n_partition = if (para.n_partition > 0) para.n_partition else sc.defaultMinPartitions

    val edges: RDD[(Long, Long, Double)] = sc.textFile(para.edge_pt, n_partition).map {
      line =>
        val Array(src, dst) = line.split("\\s+")
        (src.toLong, dst.toLong, 1.0)
    }
    val g = GraphLoader.loadGraph(edges, 0.0, dir = false).cache()

    val model = KShell.run(g, dir, weight)
    model.saveModel(para.model_pt)

    sc.stop()
  }

  /** command line parameters */
  case class Params(edge_pt: String = "",
                    model_pt: String = "",
                    dir: String = "degree",
                    n_partition: Int = -1,
                    weight: Boolean = false)
}
