package bda.spark.model.matrixFactorization

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.Rate
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Regression.RMSE
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * NMFModel with shared parameters
  *
  * @param bc_W   The left low-rank matrix
  * @param bc_H   The right low-rank matrix
  */
private[matrixFactorization]
class NMFSharedModel(val sc: SparkContext,
                     val bc_W: Broadcast[Map[Int, DenseVector[Double]]],
                     val bc_H: Broadcast[Map[Int, DenseVector[Double]]])
  extends NMFModel {

  val K = bc_W.value.head._2.size

  val W: Map[Int, DenseVector[Double]] = bc_W.value
  val H: Map[Int, DenseVector[Double]] = bc_H.value

  /** Use the mean of ws as the latent factor for missing rows */
  private val mean_ws = W.values.reduce(_ + _) / W.size

  /** Use the mean of hs as the latent factor for missing columns */
  private val mean_hs = H.values.reduce(_ + _) / H.size

  /**
    * Predict the value of R(i, j) in local. If i or j not recorded in the model,
    * then use the mean value of latent factors to predict.
    */
  def predict(rate: Rate): Double = {
    val ws = W.getOrElse(rate.user, mean_ws)
    val hs = H.getOrElse(rate.item, mean_hs)
    ws dot hs
  }

  /**
    * Predict the values of multiply R(i, j)s. Only predict the
    * (i, j) that exists in the training data.
    */
  def predict(rates: RDD[Rate]): RDD[(Double, Rate)] = {
    val default_ws = mean_ws
    val default_hs = mean_hs
    val t_W = bc_W
    val t_H = bc_H
    rates.map { rate =>
      val ws = t_W.value.getOrElse(rate.user, default_ws)
      val hs = t_H.value.getOrElse(rate.item, default_hs)
      (ws dot hs, rate)
    }
  }

  /**
    * Predict the values of multiply R(i, j)s. Only predict the
    * (i, j) that exists in the training data.
 *
    * @param  rds  (i, j, label)
    * @return  (label, prediction)
    */
  def predictWithLabel(rds: RDD[Rate]): RDD[(Double, Double)] = {
    val default_ws = mean_ws
    val default_hs = mean_hs
    val t_W = bc_W
    val t_H = bc_H
    rds.map {
      case Rate(i, j, r) =>
        val ws = t_W.value.getOrElse(i, default_ws)
        val hs = t_H.value.getOrElse(j, default_hs)
        (r, ws dot hs)
    }
  }

  /** Save the model into model_pt, which will output two files: model.W and model.H */
  def save(model_pt: String): Unit = {
    val rdd = sc.makeRDD(Seq(W, H))
    logInfo("Save model:" + model_pt)
    rdd.saveAsObjectFile(model_pt)
  }
}

object NMFSharedModel extends Logging {

  /**
    * Random initialize a model
 *
    * @param K  The reduced rank
    * @param users   The sequence of user ids
    * @param items   The sequence of item ids
    * @param std  The standard variance of initialization values
    * @return  A NMFSharedModel
    * @note It is observed that initializing all elements to miu is better
    *       than random initialization. Thus, random initialization is abandoned.
    */
  def rand(sc: SparkContext,
           K: Int,
           users: Seq[Int],
           items: Seq[Int],
           std: Double = 0.01): NMFSharedModel = {
    val W = users.map { i => (i, DenseVector.ones[Double](K) * std) }.toMap
    val H = items.map { j => (j, DenseVector.ones[Double](K) * std) }.toMap
    val bc_W = sc.broadcast(W)
    val bc_H = sc.broadcast(H)
    new NMFSharedModel(sc, bc_W, bc_H)
  }

  /** Random initialize a model from a training data set. */
  def rand(rates: RDD[Rate], K: Int): NMFSharedModel = {
    val n_train = rates.count()
    // Set the initial value of latent matrices to sqrt(mean(R)/K).
    // Then their product will be close to the mean of entries in R.
    val miu = math.sqrt(rates.map(_.label).sum / n_train / K)
    val users = rates.map(_.user).distinct().collect().toSeq
    val items = rates.map(_.item).distinct().collect().toSeq
    rand(rates.context, K, users, items, miu)
  }

  /**
    * Load a saved model
    *
    * @param model_pt  model file path
    */
  def load(sc: SparkContext, model_pt: String): NMFSharedModel = {
    logInfo("load model from: $model_pt")
    val arr = sc.objectFile[Any](model_pt).collect()
    val bc_W = sc.broadcast(arr(0).asInstanceOf[Map[Int, DenseVector[Double]]])
    val bc_H = sc.broadcast(arr(1).asInstanceOf[Map[Int, DenseVector[Double]]])
    new NMFSharedModel(sc, bc_W, bc_H)
  }
}

/**
  * Row-based gradient descent trainer for NMF
  *
  * @param K  The dimension of the low-rank matrices
  * @param learn_rate  Learn rate (i.e., step size) in gradient descent
  * @param reg  Regularization coefficient
  * @param max_iter  Number of iterations
  */
class NMFSharedTrainer(val K: Int,
                       val learn_rate: Double,
                       val reg: Double,
                       val max_iter: Int)
  extends NMFTrainer {

  /**
    * Train a NMF model via Gradient descent
    */
  def train(train_rds: RDD[Rate],
            valid_rds: RDD[Rate] = null): NMFModel = {
    if (train_rds.getStorageLevel == StorageLevel.NONE)
      logWarn("train_rds is not cached!")

    if (valid_rds != null) {
      if (valid_rds.getStorageLevel == StorageLevel.NONE)
        logWarn("valid_rds is not cached!")
    }

    var model = NMFSharedModel.rand(train_rds, K)
    val trainRMSE = evaluate(train_rds, model)
    val msg = Msg("iter" -> 0, "trainRMSE" -> trainRMSE)
    if (valid_rds != null) {
      val testRMSE = evaluate(valid_rds, model)
      msg.append("testRMSE", testRMSE)
    }
    logInfo(msg.toString)

    val times = new ArrayBuffer[Double]()
    for (iter <- 1 to max_iter) {
      val timer = new Timer()

      // update the parameters
      val lrate = learn_rate / math.sqrt(iter)
      model = update(train_rds, model, lrate)

      val cost_t = timer.cost()
      times.append(cost_t)

      val trainRMSE = evaluate(train_rds, model)
      val msg = Msg("iter" -> iter,
        "time cost" -> cost_t,
        "trainRMSE" -> trainRMSE)
      if (valid_rds != null) {
        val testRMSE = evaluate(valid_rds, model)
        msg.append("testRMSE", testRMSE)
      }
      logInfo(msg.toString)
    }

    logInfo("average iteration time:" + times.sum / times.size + "ms")
    model
  }

  /** A single iteration of parameter update */
  private def update(rds: RDD[Rate],
                     model: NMFSharedModel,
                     lrate: Double): NMFSharedModel = {
    val sc = rds.context
    val W = model.bc_W
    val H = model.bc_H
    val m_W = updateW(rds, W, H, lrate)
    W.unpersist()

    // update H with new_W
    val new_W = sc.broadcast(m_W)
    val m_H = updateH(rds, new_W, H, lrate)
    H.unpersist()
    val new_H = sc.broadcast(m_H)
    new NMFSharedModel(sc, new_W, new_H)
  }

  /**
    * Update the Matrix W with fixed H
 *
    * @param rds   Training records, i.e., (i, j, v)-triples
    * @param W   the left low-rank matrix, (colId: Int, DenseVector(K))
    * @param H   the right low-rank matrix, (colId: Int, DenseVector(K))
    * @param lrate learn rate
    * @return   A new left low-rank matrix, (rowId: Int, DenseVector(K))
    */
  private def updateW(rds: RDD[Rate],
                      W: Broadcast[Map[Int, DenseVector[Double]]],
                      H: Broadcast[Map[Int, DenseVector[Double]]],
                      lrate: Double
                     ): Map[Int, DenseVector[Double]] = {
    val grad_W: RDD[(Int, DenseVector[Double])] = rds.map {
      case Rate(i, j, label) =>
        // error vector of user u
        val e = label - W.value(i).dot(H.value(j))
        (i, H.value(j) * e)
    }.reduceByKey(_ + _)

    // update W
    val r = reg
    grad_W.map {
      case (i, grad_ws) =>
        val new_ws = W.value(i) * (1 - lrate * r) + grad_ws * lrate
        (i, new_ws.map(math.max(_, 0.0)))
    }.collect().toMap
  }

  /**
    * Update H with fixed W
    */
  private def updateH(rds: RDD[Rate],
                      W: Broadcast[Map[Int, DenseVector[Double]]],
                      H: Broadcast[Map[Int, DenseVector[Double]]],
                      lrate: Double): Map[Int, DenseVector[Double]] = {
    // calculate and collect the gradient of H
    val grad_H: RDD[(Int, DenseVector[Double])] = rds.map {
      case Rate(i, j, label) =>
        val e = label - W.value(i).dot(H.value(j))
        (j, W.value(i) * e)
    }.reduceByKey(_ + _)

    // update H
    val r = reg
    grad_H.map {
      case (j, grad_hs) =>
        val new_hs = H.value(j) * (1 - lrate * r) + grad_hs * lrate
        (j, new_hs.map(math.max(_, 0)))
    }.collect().toMap
  }

  /**
    * Evaluate the training RMSE
    */
  private def evaluate(rds: RDD[Rate],
                       model: NMFSharedModel): Double = {
    val tys = model.predict(rds).map {
      case (prediction, rate) => (rate.label, prediction)
    }
    RMSE(tys)
  }

}