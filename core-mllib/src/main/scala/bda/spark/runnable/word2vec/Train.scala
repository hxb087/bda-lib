package bda.spark.runnable.word2vec

import bda.common.obj.{RawDoc, Doc}
import bda.spark.model.word2vec.{Word2Vec, Word2VecModel}
import bda.spark.preprocess.{WordIndex, WordFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

object Train {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("Spark Word2Vec Train") {
      head("Train", "1.0")
      opt[String]("train_pt").required()
        .text("The path of training file. Each line is a document, represented by word index sequences, e.g., \"wid wid ...\"")
        .action { (x, c) => c.copy(train_pt = x) }

      opt[String]("model_pt")
        .text("The path to write the model (word representations).")
        .action { (x, c) => c.copy(model_pt = x) }

      opt[Int]("K")
        .text("The size of vector for word representation, default is 100")
        .action { (x, c) => c.copy(K = x) }
        .validate {
          x => if (x > 0) success else failure("Option --K must > 0")
        }

      opt[Double]("learn_rate")
        .text("The init learning rate of gradient decent, default is 0.025")
        .action { (x, c) => c.copy(learn_rate = x) }
        .validate { x =>
          if (x > 0.00) success else failure("Option --learn_rate must > 0.00")
        }

      opt[Int]("max_window")
        .text("The max size of sliding window, default is 3")
        .action { (x, c) => c.copy(max_window = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --max_window must > 0")
        }

      opt[Int]("iter")
        .text("The number of iteration, default is 5")
        .action { (x, c) => c.copy(iter = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --iter must > 0")
        }

      opt[Double]("subsample_coef")
        .text("The coefficient of sub-sampling. Set threshold for occurrence of words. Those that appear with higher frequency in the training data. default is 1e-2")
        .action { (x, c) => c.copy(subsample_coef = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --subsample_coef must > 0.0")
        }

      opt[Int]("n_negative")
        .text("The number of negative sample, default is 5")
        .action { (x, c) => c.copy(n_negative = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --n_negative must > 0")
        }

      opt[Int]("min_count")
        .text("The minimum frequency of word, default is 5")
        .action { (x, c) => c.copy(min_count = x) }
        .validate { x =>
          if (x > 0) success else failure("Option --min_count must > 0")
        }
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(-1)
    }
  }

  def run(p: Params): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark Word2Vec Training")
    val sc = new SparkContext(sparkConf)

    val (docs, w2id) = readDocs(sc, p.train_pt, p.min_count)

    val model: Word2VecModel = Word2Vec.train(docs,
      w2id = w2id,
      K = p.K,
      max_window = p.max_window,
      n_negative = p.n_negative,
      subsample_coef = p.subsample_coef,
      max_iter = p.iter)

    model.save(p.model_pt)
  }

  private def readDocs(sc: SparkContext,
                       train_pt: String,
                       min_count: Int): (RDD[Doc], Map[String, Int]) = {

    val corpus: RDD[RawDoc] = sc.textFile(train_pt).zipWithIndex.map {
      case (ln, index) =>
        new RawDoc(index.toString, RawDoc.default_label, ln.split("\\s+"))
    }

    val rawDocs = WordFilter(corpus, min_freq = min_count)
    rawDocs.cache()
    WordIndex(rawDocs)
  }

  case class Params(train_pt: String = "",
                    model_pt: String = "",
                    K: Int = 100,
                    learn_rate: Double = 0.025,
                    max_window: Int = 3,
                    iter: Int = 5,
                    subsample_coef: Double = 1e-2,
                    n_negative: Int = 5,
                    min_count: Int = 5)

}
