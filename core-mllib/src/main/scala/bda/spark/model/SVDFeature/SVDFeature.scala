package bda.spark.model.SVDFeature

import bda.common.Logging
import bda.common.obj.SVDFeaturePoint
import bda.common.util.Msg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Abstract class for SVDFeature Model
  */
abstract class SVDFeatureModel extends Logging {
  /** The dimension of latent representation of user/item features */
  val K: Int

  /** Number of user features */
  val Fu: Int

  /** Number of item features */
  val Fi: Int

  /** Number of item features */
  val Fg: Int

  /** Regression task or classification task */
  val is_regression: Boolean

  /** Predict the ys for points */
  protected def evalY(ps: RDD[SVDFeaturePoint]): RDD[Double]

  /**
    * Predict the ys for points
    * @param ps  SVDFeature points
    * @return  RDD[(true label, predict label)]
    */
  protected def evalYWithLabel(ps: RDD[SVDFeaturePoint]): RDD[(Double, Double)]

  /** Predict the labels for points */
  def predict(ps: RDD[SVDFeaturePoint]): RDD[Double] = if (is_regression)
    evalY(ps)
  else
    evalY(ps).map {
      math.signum(_)
      //math.signum
    }

  /** Predict the labels for points while retaining the original label */
  def predictWithLabel(ps: RDD[SVDFeaturePoint]): RDD[(Double, Double)] = if (is_regression)
    evalYWithLabel(ps)
  else
    evalYWithLabel(ps).map { case (t, y) => (t, math.signum(y)) }

  /** Save the model into files */
  def save(model_pt: String): Unit
}

object SVDFeatureModel {

  def load(sc: SparkContext,
           model_pt: String,
           graphx: Boolean = false): SVDFeatureModel = {
    if (graphx)
      throw new IllegalArgumentException("GraphX SVDFeature has not been implemented!")
    else
      SVDFeatureSharedModel.load(sc, model_pt)
  }
}

private[SVDFeature]
abstract class SVDFeatureTrainer extends Logging {

  def train(train_pns: RDD[SVDFeaturePoint],
            test_pns: RDD[SVDFeaturePoint] = null): SVDFeatureModel
}

/** User interface for Spark SVDFeature */
object SVDFeature extends Logging {

  /**
    * Train a SVDFeature Model
    * @param train_pns  training points
    * @param valid_pns  validation points
    * @param is_regression   "classification" or "regression"
    * @param graphx   If true, use the GraphX implementation; else
    *                 use the shared model implementation.
    * @param K   The dimension of latent representations for user/item featurs.
    * @param learn_rate  Learn rate
    * @param reg  Regularization coefficient
    * @param max_iter  The maximum number of iterations
    * @return   A SVDFeature Model
    */
  def train(train_pns: RDD[SVDFeaturePoint],
            valid_pns: RDD[SVDFeaturePoint] = null,
            is_regression: Boolean,
            graphx: Boolean = false,
            K: Int = 10,
            learn_rate: Double = 0.01,
            reg: Double = 0.1,
            max_iter: Int = 100): SVDFeatureModel = {
    if (train_pns.getStorageLevel == StorageLevel.NONE)
      logWarn("train_pns is not cached!")
    if (valid_pns != null && valid_pns.getStorageLevel == StorageLevel.NONE)
      logWarn("valid_pns is not cached!")

    // print the parameter information
    val Fu = train_pns.map(_.u_fs.maxActiveIndex).max + 1
    val Fi = train_pns.map(_.i_fs.maxActiveIndex).max + 1
    val Fg = train_pns.map(_.g_fs.maxActiveIndex).max + 1
    val n_valid = if (valid_pns == null) 0 else valid_pns.count()
    val msg = Msg("n(train)" -> train_pns.count(),
      "n(validate)" -> n_valid,
      "is_regression" -> is_regression,
      "graphx" -> graphx,
      "K" -> K,
      "n(userFeature)" -> Fu,
      "n(itemFeature)" -> Fi,
      "n(globalFeature)" -> Fg,
      "learn_rate" -> learn_rate,
      "reg" -> reg,
      "max_iter" -> max_iter
    )
    logInfo(msg.toString)

    val trainer = if (graphx)
      throw new IllegalArgumentException("GraphX SVDFeature has not been implemented!")
    else
      new SVDFeatureSharedTrainer(is_regression, K, max_iter, learn_rate, reg)

    trainer.train(train_pns, valid_pns)
  }
}