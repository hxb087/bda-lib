package bda.spark.model.logisticRegression

import bda.common.Logging
import bda.common.linalg.{DenseMatrix, DenseVector}
import bda.common.obj.LabeledPoint
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Classification.accuracy
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Logistic regression with a shared model
  */
private[logisticRegression]
class LRSharedModel(sc: SparkContext,
                    val bc_bs: Broadcast[DenseVector[Double]],
                    val bc_W: Broadcast[DenseMatrix[Double]])
  extends LRModel with Logging {

  val bs: DenseVector[Double] = bc_bs.value
  val W: DenseMatrix[Double] = bc_W.value

  /** The number of classes */
  val class_num: Int = bs.size

  /** The feature size */
  val feature_num: Int = W.colNum

  /**
    * Evaluate P(y=k|point) = softmax(bs + w * fs) for a single point
 *
    * @return   The probability belongs to each class, a C-dim DenseVector
    */
  protected def evalY(point: LabeledPoint): DenseVector[Double] =
    bs + W.dot(point.fs)

  /** Evaluate P(y|point) for a point collection */
  protected def evalY(points: RDD[LabeledPoint]): RDD[DenseVector[Double]] = {
    // use local variables to avoid serialize the whole object
    val t_bs = bs
    val t_W = W
    points.map {
      point => t_bs + t_W.dot(point.fs)
    }
  }

  /** Save the model into model_pt, which will output two files: model.bs and model.W */
  def save(model_pt: String): Unit = {
    logInfo(s"save model: $model_pt")
    sc.parallelize(Seq(bs, W)).saveAsObjectFile(model_pt)
  }
}

private[logisticRegression] object LRSharedModel extends Logging {

  /** Create a randomly initialized model */
  def rand(sc: SparkContext,
           class_num: Int,
           feature_num: Int): LRSharedModel = {
    val bs = DenseVector.zeros[Double](class_num)
    val W = DenseMatrix.zeros[Double](class_num, feature_num)
    val bc_bs = sc.broadcast(bs)
    val bc_W = sc.broadcast(W)
    new LRSharedModel(sc, bc_bs, bc_W)
  }

  /**
    * Load a saved model
 *
    * @param model_pt   model file path
    */
  def load(sc: SparkContext, model_pt: String): LRSharedModel = {
    logInfo("load model from: $model_pt")
    val arr = sc.objectFile[Any](model_pt).collect()
    val bs = arr(0).asInstanceOf[DenseVector[Double]]
    val W = arr(1).asInstanceOf[DenseMatrix[Double]]
    val bc_bs = sc.broadcast(bs)
    val bc_W = sc.broadcast(W)
    new LRSharedModel(sc, bc_bs, bc_W)
  }
}

/**
  * Trainer class for spark logistic regression
  */
private[logisticRegression]
class LRSharedTrainer(val class_num: Int,
                      val feature_num: Int,
                      val max_iter: Int,
                      val learn_rate: Double,
                      val reg: Double)
  extends LRTrainer {
  /**
    * Training a LRModel over `train_points`.
    * If `valid_points` is provided, validate the trained model during training.
    */
  def train(train_points: RDD[LabeledPoint],
            valid_points: RDD[LabeledPoint] = null): LRModel = {
    if (train_points.getStorageLevel == StorageLevel.NONE) {
      train_points.cache()
      //logWarn("train_points are not cached.")
    }

    if (valid_points != null &&
      valid_points.getStorageLevel == StorageLevel.NONE) {
      valid_points.cache()
      //logWarn("valid_points are not cached.")
    }

    val sc = train_points.context
    var model = LRSharedModel.rand(sc, class_num, feature_num)

    val msg = Msg("iter" -> 0,
      "accuracy(train)" -> evaluate(model, train_points))
    if (valid_points != null)
      msg.append("accuracy(validate)", evaluate(model, valid_points))
    logInfo(msg.toString)

    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      model = update(train_points, model, iter)

      // evaluate the trained model
      val time_cost = timer.cost
      val msg = Msg("iter" -> iter,
        "time cost(ms)" -> time_cost,
        "accuracy(train)" -> evaluate(model, train_points))
      if (valid_points != null)
        msg.append("accuracy(validate)", evaluate(model, valid_points))


      logInfo(msg.toString)
    }
    model
  }

  /**
    * Update the model with a single gradient descent step
 *
    * @param points  Training points
    * @param model   The original model
    * @param iter    Iteration number
    * @return  A new Model
    */
  protected def update(points: RDD[LabeledPoint],
                       model: LRSharedModel,
                       iter: Int): LRSharedModel = {
    // Gradient descent update
    val (grad_bs, grad_W) = computeGradient(points, model)

    // adjust the learning rate with iteration number
    val lrate = learn_rate / math.sqrt(iter)
    val new_bs = model.bs * (1 - lrate * reg) + grad_bs * lrate
    val new_W = model.W * (1 - lrate * reg) + grad_W * lrate

    val sc = points.context
    val bc_bs = sc.broadcast(new_bs)
    val bc_W = sc.broadcast(new_W)
    new LRSharedModel(sc, bc_bs, bc_W)
  }

  /** Compute the gradient of model over points */
  private def computeGradient(points: RDD[LabeledPoint],
                              model: LRSharedModel
                             ): (DenseVector[Double], DenseMatrix[Double]) = {
    // compute the gradient on each partition and then aggregate
    val ys: RDD[DenseVector[Double]] = model.evalClassProbability(points)

    val C = model.class_num
    val F = model.feature_num
    val (g_bs, g_W) = points.zip(ys).mapPartitions { pnts =>
      val grad_bs = new DenseVector[Double](C)
      val grad_W = new DenseMatrix[Double](C, F)
      pnts.foreach {
        case (pnt, y) =>
          for (c <- 0 until C) {
            val e = if (c == math.round(pnt.label)) 1 - y(c) else -y(c)
            grad_bs(c) += e
            pnt.fs.foreachActive {
              case (f, v) => grad_W(c, f) += e * v
            }
          }
      } // end gradient collection
      Iterator((grad_bs, grad_W))
    }.treeReduce((a, b) => (a._1 + b._1, a._2 + b._2))
    (g_bs, g_W)
  }

  /**
    * Evaluate the accuracy of `model` over data `points`
    */
  private def evaluate(model: LRSharedModel,
                       points: RDD[LabeledPoint]): Double = {
    val ys = model.predict(points)
    val tys: RDD[(Double, Double)] = points.zip(ys).map {
      case (pnt, y) =>
        (pnt.label, y.toDouble)
    }
    accuracy(tys)
  }
}

