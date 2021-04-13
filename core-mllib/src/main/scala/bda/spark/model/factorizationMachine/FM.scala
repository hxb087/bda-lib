package bda.spark.model.factorizationMachine

import bda.common.Logging
import bda.common.obj.LabeledPoint
import bda.common.util.Msg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Abstract class for FM Trainer.
 */
private[factorizationMachine]
abstract class FMTrainer extends Logging {

  def train(train_data: RDD[LabeledPoint],
            valid_data: RDD[LabeledPoint]): FMModel
}

/**
 * Abstract Model for factorization machine.
 */
abstract class FMModel {

  def predict(test: RDD[LabeledPoint]): RDD[(Double, Double)]

  def predict(test: LabeledPoint): Double

  def saveModel(sc: SparkContext, model_pt: String): Unit
}

/** Class FMModel factory. */
object FMModel extends Logging {
  /**
   * A unified interface for model loading
    *
    * @param model_pt model path, either on local or hdfs
   * @param graphx  If true, load a GraphX version model.
   * @return A FMModel.
   */
  def load(sc: SparkContext,
           model_pt: String,
           graphx: Boolean = false): FMModel = {
    if (graphx)
      FMGraphxModel.loadModel(sc, model_pt)
    else
      FMSharedModel.loadModel(sc, model_pt)
  }
}

/** External interface of local factorization machine. */
object FM extends Logging{

  /**
   * A adapter for training a logistic regression model
   *
   * @param train_point Training data samples.
   * @param valid_point Validataion data samples.
   * @param is_regression    Regression or Classification
   * @param learn_rate  learnint rate,  default is 0.01
   * @param max_iter Maximum iteration
   * @param graphx If true, traing use GraphX trainer; else use shared model trainer.
   * @param dim
   * @param reg  Regularization strength, default is (0.01, 0.01).
   * @param init_std  Initialized standard deviation.
   * @return  A FM model.
   */
  def train(train_point: RDD[LabeledPoint],
            valid_point: RDD[LabeledPoint] = null,
            is_regression: Boolean,
            learn_rate: Double,
            max_iter: Int,
            graphx: Boolean = false,
            dim: (Boolean, Int) = (true, 8),
            reg: (Double, Double) = (0.0, 0.0),
            init_std: Double = 0.1): FMModel = {
    val msg = Msg("n(train_data)" -> train_point.count,
      "is_regression" -> is_regression,
      "learn_rate" -> learn_rate,
      "max_iter" -> max_iter,
      "dim" -> dim,
      "reg" -> reg,
      "init_std" -> init_std,
      "graphx" -> graphx)
    logInfo(msg.toString)

    val trainer: FMTrainer = if (graphx)
      new FMGraphxTrainer(is_regression, learn_rate, max_iter, dim, reg, init_std)
    else
      new FMSharedTrainer(is_regression, learn_rate, max_iter, dim, reg, init_std)
    trainer.train(train_point, valid_point)
  }
}