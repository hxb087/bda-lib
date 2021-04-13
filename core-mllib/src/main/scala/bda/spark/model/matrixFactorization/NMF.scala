package bda.spark.model.matrixFactorization

import bda.common.Logging
import bda.common.obj.Rate
import bda.common.util.Msg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Base class of NMFModel
  */
abstract class NMFModel extends Logging {
  /** The reduced Rank */
  val K: Int

  /** Predict the rating of (i, j) */
  def predict(rate: Rate): Double

  /** Batch prediction */
  def predict(rates: RDD[Rate]): RDD[(Double, Rate)]

  /** Save model into model_pt */
  def save(model_pt: String): Unit
}

object NMFModel {

  /**
    * A unified interface for model loading
    * @param model_pt  model path, either on local or hdfs
    * @param graphx  If true, load a GraphX version model.
    * @return  A NMFModel
    */
  def load(sc: SparkContext,
           model_pt: String,
           graphx: Boolean = false): NMFModel = {
    if (graphx)
      NMFGraphXModel.load(sc, model_pt)
    else
      NMFSharedModel.load(sc, model_pt)
  }
}

/**
  * Base class of NMF Trainer
  */
private[matrixFactorization]
abstract class NMFTrainer extends Logging {

  def train(train_points: RDD[Rate],
            valid_points: RDD[Rate] = null): NMFModel
}

/** User interface */
object NMF extends Logging {

  /**
    * Spark NMF training
    * @param train_points  training data points
    * @param valid_points validation data points
    * @param K  The reduced rank
    * @param graphx  whether or not use GraphX
    * @param learn_rate  learn rate
    * @param reg  regularization coefficient
    * @param max_iter  the maximum number of iterations
    * @return  A NMF Model
    */
  def train(train_points: RDD[Rate],
            valid_points: RDD[Rate] = null,
            K: Int = 10,
            graphx: Boolean = false,
            learn_rate: Double = 0.001,
            reg: Double = 0.1,
            max_iter: Int = 100
           ): NMFModel = {
    val n_valid = if (valid_points == null) 0 else valid_points.count()
    val msg = Msg("n(train)" -> train_points.count(),
      "n(validate)" -> n_valid,
      "K" -> K,
      "graphx" -> graphx,
      "learn_rate" -> learn_rate,
      "reg" -> reg,
      "max_iter" -> max_iter
    )
    logInfo(msg.toString)

    val trainer = if (graphx)
      new NMFGraphXTrainer(K, learn_rate, reg, max_iter)
    else
      new NMFSharedTrainer(K, learn_rate, reg, max_iter)

    trainer.train(train_points, valid_points)
  }
}