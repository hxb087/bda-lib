package bda.spark.model.logisticRegression

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.LabeledPoint
import bda.common.util.Msg
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Abstract Model for logistic regression
  */
abstract class LRModel {

  /** The number of classes */
  val class_num: Int

  /** The feature size */
  val feature_num: Int

  /** Evaluate P(y|p) of each class for a point */
  def evalClassProbability(p: LabeledPoint): DenseVector[Double] =
    evalY(p).expNormalize()

  /** Evaluate P(y|p) of each class for a point */
  def evalClassProbability(ps: RDD[LabeledPoint]): RDD[DenseVector[Double]] =
    evalY(ps).map(_.expNormalize())

  /** Predict the label, given a feature vector */
  def predict(p: LabeledPoint): Int = evalY(p).argmax

  /** Batch prediction */
  def predict(ps: RDD[LabeledPoint]): RDD[Int] = evalY(ps).map(_.argmax)

  /** Save the model into file */
  def save(model_pt: String): Unit

  /**
    * Compute P(y=k|fs) = softmax(bs + w * fs)
    * @return   The probability belongs to each class, a C-dim DenseVector
    */
  protected def evalY(point: LabeledPoint): DenseVector[Double]

  /** Evaluate P(y|point) for a point collection */
  protected def evalY(points: RDD[LabeledPoint]): RDD[DenseVector[Double]]
}

object LRModel {

  /**
    * A unified interface for model loading
    * @param model_pt  model path, either on local or hdfs
    * @param graphx  If true, load a GraphX version model.
    * @return  A LRModel object
    */
  def load(sc: SparkContext,
           model_pt: String,
           graphx: Boolean = false): LRModel = {
    if (graphx)
      LRGraphXModel.load(sc, model_pt)
    else
      LRSharedModel.load(sc, model_pt)
  }
}

/**
  * Abstract class for logistic trainer
  */
private[logisticRegression]
abstract class LRTrainer extends Logging {

  def train(train_points: RDD[LabeledPoint],
            valid_points: RDD[LabeledPoint] = null): LRModel
}

/** External interface of local logistic regression */
object LR extends Logging {

  /**
    * A adapter for training a logistic regression model
    *
    * @param train_points   Training data points.
    * @param valid_points   Validation data points, default is None
    * @param class_num     Number of classes. If is -1, determine it from train_points.
    * @param graphx   If true, traing use GraphX trainer; else use shared model trainer.
    * @param max_iter      Maximum number of iterations, default is 100.
    * @param learn_rate    Learn rate, default is 0.01.
    * @param reg    Regularization strength, default is 0.01.
    * @return  A LRModel
    */
  def train(train_points: RDD[LabeledPoint],
            valid_points: RDD[LabeledPoint] = null,
            class_num: Int = -1,
            graphx: Boolean = false,
            max_iter: Int = 10,
            learn_rate: Double = 0.01,
            reg: Double = 0.01): LRModel = {
    //计算多分类数量
    val C: Int = if (class_num == -1) {
      // determine the number of class by the maximum classID
      train_points.map(pn => math.round(pn.label).toInt).max + 1
    } else class_num
    //计算labelpoint中feature中的最大索引值
    val F: Int = train_points.map(_.fs.maxActiveIndex).max + 1
    //验证集总数
    val n_valid = if (valid_points == null) 0
    else valid_points.count()

    // logging the input parameters
    val msg = Msg("n(train)" -> train_points.count(),
      "n(validate)" -> n_valid,
      "n(class)" -> C,
      "n(feature)" -> F,
      "graphx" -> graphx,
      "max_iter" -> max_iter,
      "learn_rate" -> learn_rate,
      "reg" -> reg
    )
    logInfo(msg.toString)
    // choose a trainer class
    val trainer: LRTrainer = if (graphx)
      new LRGraphXTrainer(C, F, max_iter, learn_rate, reg)
    else
      new LRSharedTrainer(C, F, max_iter, learn_rate, reg)

    trainer.train(train_points, valid_points)
  }
}