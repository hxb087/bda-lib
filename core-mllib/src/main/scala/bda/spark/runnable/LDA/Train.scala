package bda.spark.runnable.LDA

import bda.common.linalg.DenseVector
import bda.common.obj.Doc
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import bda.spark.model.LDA.LDA
import org.apache.log4j.{Level, Logger}

/**
  * Command line wrapper for Spark LDA Training
  */
object Train {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Spark LDA Train") {
      head("Train", "1.0")
      opt[String]("train_pt").required()
        .text("The path of training file. Each line is a document, represented by word index sequences, e.g., \"wid wid ...\"")
        .action { (x, c) => c.copy(train_pt = x) }
      opt[Int]("K")
        .text("The number of topics, default is 10.")
        .action { (x, c) => c.copy(K = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --K must > 0")
        }
      opt[Double]("alpha")
        .text("Dirichlet prior of P(z|d).")
        .action { (x, c) => c.copy(alpha = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --alpha must > 0")
        }
      opt[Double]("beta")
        .text("Dirichlet prior of P(w|z).")
        .action { (x, c) => c.copy(beta = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --beta must > 0")
        }
      opt[Boolean]("graphx")
        .text("If true, using the GraphX implementation; Else use the shared model implementation. Default is false.")
        .action { (x, c) => c.copy(graphx = x) }
      opt[Int]("max_iter")
        .text("The max number of iterations, default is 100")
        .action { (x, c) => c.copy(max_iter = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --max_iter must > 0")
        }
      opt[Int]("n_partition")
        .text("The size of vocabulary, default is -1, which will be determined from the training documents.")
        .action { (x, c) => c.copy(n_partition = x) }
      opt[String]("model_pt")
        .text("The path to write the model (word distribution of the topics).")
        .action { (x, c) => c.copy(model_pt = x) }
      opt[String]("pz_d_pt")
        .text("The path to write the P(z|d) of each document.")
        .action { (x, c) => c.copy(pz_d_pt = x) }
      help("help").text("prints this usage text")
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark LDA Training")
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(sparkConf)

    val n_partition = if (p.n_partition > 0)
      p.n_partition
    else
      sc.defaultMinPartitions

    val docs = sc.textFile(p.train_pt, n_partition).map(Doc.parse).cache()
    val model = LDA.train(docs, p.K, p.alpha, p.beta,
      p.max_iter, p.graphx)

    if (!p.model_pt.isEmpty)
      model.save(p.model_pt)

    // infer p(z|d) and write into pz_d_pt
    if (!p.pz_d_pt.isEmpty) {
      val pz_d: RDD[DenseVector[Double]] = model.predict(docs)
      pz_d.saveAsTextFile(p.pz_d_pt)
    }
  }

  /** command line parameters */
  case class Params(train_pt: String = "",
                    model_pt: String = "",
                    pz_d_pt: String = "",
                    K: Int = 10,
                    alpha: Double = -1.0,
                    beta: Double = 0.01,
                    max_iter: Int = 100,
                    n_partition: Int = -1,
                    graphx: Boolean = false)

}
