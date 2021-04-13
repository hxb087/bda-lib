package bda.spark.model.SVDFeature

import bda.common.Logging
import bda.common.linalg.{DenseMatrix, DenseVector}
import bda.common.obj.SVDFeaturePoint
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Classification.accuracy
import bda.spark.evaluate.Regression.RMSE
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


/**
  * SVDFeature Model definition.
 *
  * @note The feature id are indexed as 0, 1, 2, ...
  * @param b   bias
  * @param bc_wu  Broadcast variable of weights of user features
  * @param bc_wi  Broadcast variable of weights of item features
  * @param bc_wg  Broadcast variable of weights of global features
  * @param bc_U   Broadcast variable of latent factors of user features
  * @param bc_I   Broadcast variable of latent factors of item features
  */
private[SVDFeature]
class SVDFeatureSharedModel(val sc: SparkContext,
                            val is_regression: Boolean,
                            val b: Double,
                            val bc_wu: Broadcast[DenseVector[Double]],
                            val bc_wi: Broadcast[DenseVector[Double]],
                            val bc_wg: Broadcast[DenseVector[Double]],
                            val bc_U: Broadcast[DenseMatrix[Double]],
                            val bc_I: Broadcast[DenseMatrix[Double]])
  extends SVDFeatureModel {

  def this(sc: SparkContext,
           is_regression: Boolean,
           b: Double,
           wu: DenseVector[Double],
           wi: DenseVector[Double],
           wg: DenseVector[Double],
           U: DenseMatrix[Double],
           I: DenseMatrix[Double]) = this(sc, is_regression, b, sc.broadcast(wu),
    sc.broadcast(wi), sc.broadcast(wg), sc.broadcast(U), sc.broadcast(I))

  val wu = bc_wu.value
  val wi = bc_wi.value
  val wg = bc_wg.value
  val U = bc_U.value
  val I = bc_I.value

  override val K: Int = U.colNum

  override val Fu: Int = wu.size
  override val Fi: Int = wi.size
  override val Fg: Int = wg.size

  /** Predict the labels for points */
  override def evalY(ps: RDD[SVDFeaturePoint]): RDD[Double] = {
    val (t_b, t_wu, t_wi, t_wg, t_U, t_I) = (b, bc_wu, bc_wi, bc_wg, bc_U, bc_I)
    ps.mapPartitions { p_ps =>
      val (local_b, local_wu, local_wi, local_wg, local_U, local_I) =
        (t_b, t_wu.value, t_wi.value, t_wg.value, t_U.value, t_I.value)
      p_ps.map { p =>
        local_b + (local_wu dot p.u_fs) + (local_wi dot p.i_fs)
        + (local_wg dot p.g_fs) + ((local_U dot p.u_fs) dot (local_I dot p.i_fs))
      }
    }
  }

  /** Predict the labels for points */
  override def evalYWithLabel(ps: RDD[SVDFeaturePoint]): RDD[(Double, Double)] = {
    val (t_b, t_wu, t_wi, t_wg, t_U, t_I) = (b, bc_wu, bc_wi, bc_wg, bc_U, bc_I)
    ps.mapPartitions { p_ps =>
      val (local_b, local_wu, local_wi, local_wg, local_U, local_I) =
        (t_b, t_wu.value, t_wi.value, t_wg.value, t_U.value, t_I.value)
      p_ps.map { p =>
        val y_u = local_wu dot p.u_fs
        val y_i = local_wi dot p.i_fs
        val y_g = local_wg dot p.g_fs
        val y_ui = (local_U dot p.u_fs) dot (local_I dot p.i_fs)
        val y = local_b + y_u + y_i + y_g + y_ui
        (p.label, y)
      }
    }
  }

  /** Generate a String representation of the model */
  override def toString: String =
    s"""  b: $b
        | wu: norm=${wu.norm}, max=${wu.maxValue}, min=${wu.minValue}
        | wi: norm=${wi.norm}, max=${wi.maxValue}, min=${wi.minValue}
        |  U: norm=${U.norm}, max=${U.maxValue}, min=${U.minValue}
        |  I: norm=${I.norm}, max=${I.maxValue}, min=${I.minValue}
     """.stripMargin

  /** Save the model into files */
  override def save(model_pt: String): Unit = {
    logInfo("Save model into: " + model_pt)
    val rdd = sc.makeRDD(Array(b, is_regression, wu, wi, wg, U, I))
    rdd.saveAsObjectFile(model_pt)
  }
}

object SVDFeatureSharedModel extends Logging {
  /**
    * Random initialize the model parameters
 *
    * @note the bias is fixed
    * @param Fu  number of user features
    * @param Fi  number of item features
    * @param Fg  number of global features
    * @param K  the dimension of the latent representation of user/item features
    * @param b  bias
    * @return A SVDFeatureSharedModel
    */
  def rand(sc: SparkContext,
           is_regression: Boolean,
           K: Int, Fu: Int, Fi: Int, Fg: Int, b: Double): SVDFeatureSharedModel = {
    val bc_wu = sc.broadcast(DenseVector.zeros[Double](Fu))
    val bc_wi = sc.broadcast(DenseVector.zeros[Double](Fi))
    val bc_wg = sc.broadcast(DenseVector.zeros[Double](Fg))

    val std = math.sqrt(1.0 / K)
    val bc_U = sc.broadcast(DenseMatrix.rand(K, Fu) * std)
    val bc_I = sc.broadcast(DenseMatrix.rand(K, Fi) * std)
    new SVDFeatureSharedModel(sc, is_regression, b, bc_wu, bc_wi, bc_wg, bc_U, bc_I)
  }

  /** load a model from file */
  def load(sc: SparkContext, model_pt: String): SVDFeatureSharedModel = {
    logInfo("Load model from: " + model_pt)
    val vs = sc.objectFile[Any](model_pt).collect()
    val b = vs(0).asInstanceOf[Double]
    val is_regression = vs(1).asInstanceOf[Boolean]
    val wu = vs(2).asInstanceOf[DenseVector[Double]]
    val wi = vs(3).asInstanceOf[DenseVector[Double]]
    val wg = vs(4).asInstanceOf[DenseVector[Double]]
    val U = vs(5).asInstanceOf[DenseMatrix[Double]]
    val I = vs(6).asInstanceOf[DenseMatrix[Double]]
    new SVDFeatureSharedModel(sc, is_regression, b, wu, wi, wg, U, I)
  }
}

/**
  * Data parallel implementation of SVDTrainer
  *
  * Procedure:
  * 1. The model are broadcast to each worker.
  * 2. Each worker use local to training data to compute the gradients,
  * and aggregate them in driver.
  * 3. Then, driver perform gradient descent to update and re-broadcast the model.
  */
class SVDFeatureSharedTrainer(val is_regression: Boolean,
                              val K: Int,
                              val max_iter: Int,
                              val learn_rate: Double,
                              val reg: Double)
  extends SVDFeatureTrainer {

  /** String representation of the evaluation metric */
  private val metric: String = if (is_regression) "RMSE" else "accuracy"

  /** Train a model */
  def train(train_pns: RDD[SVDFeaturePoint],
            test_pns: RDD[SVDFeaturePoint] = null): SVDFeatureSharedModel = {
    // compute the fixed bias
    val b: Double = if (is_regression) {
      train_pns.map(_.label).sum / train_pns.count().toDouble
    } else {
      val n_neg = train_pns.filter(rd => math.abs(rd.label + 1.0) < 1e-8).count()
      val n_pos = train_pns.count() - n_neg
      math.log(n_pos.toDouble / n_neg)
    }

    // determine the number of user/item/global features
    val Fu = train_pns.map(_.u_fs.maxActiveIndex).max + 1
    val Fi = train_pns.map(_.i_fs.maxActiveIndex).max + 1
    val Fg = train_pns.map(_.g_fs.maxActiveIndex).max + 1
    var model = SVDFeatureSharedModel.rand(train_pns.context, is_regression,
      K, Fu, Fi, Fg, b)

    val times = new ArrayBuffer[Double]()
    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      val lrate = learn_rate / math.sqrt(iter)
      model = update(train_pns, model, lrate)

      val cost_t = timer.cost
      times.append(cost_t)
      val msg = Msg("iter" -> iter)

      val train_s = evaluate(train_pns, model)
      msg.append(s"$metric(train)", train_s)
      if (test_pns != null) {
        val test_s = evaluate(test_pns, model)
        msg.append(s"$metric(test)", test_s)
      }
      logInfo(msg.toString)
    }
    logInfo("average iteration time:" + times.sum / times.size + "ms")
    model
  }

  /** GD update the model by traversing the records */
  private def update(pns: RDD[SVDFeaturePoint],
                     model: SVDFeatureSharedModel,
                     lrate: Double): SVDFeatureSharedModel = {
    var mod = updateGlobal(pns, model, lrate)
    mod = updateUser(pns, mod, lrate)
    mod = updateItem(pns, mod, lrate)
    mod
  }

  /** Update the bias and weights for global feature */
  private def updateGlobal(points: RDD[SVDFeaturePoint],
                           model: SVDFeatureSharedModel,
                           lrate: Double): SVDFeatureSharedModel = {
    val es = computeErrors(points, model)
    val Fg = model.Fg // number of global features
    val grad_wg: DenseVector[Double] = points.zip(es).mapPartitions { pes =>
        val grad = DenseVector.zeros[Double](Fg)
        pes.foreach {
          case (pnt, e) =>
            grad += (pnt.g_fs * e)
        }
        Iterator(grad)
      }.reduce(_ + _)

    val new_wg = model.wg * (1 - lrate * reg) + grad_wg * lrate
    val bc_wg = model.sc.broadcast(new_wg)
    new SVDFeatureSharedModel(model.sc, is_regression,
      model.b, model.bc_wu, model.bc_wi, bc_wg, model.bc_U, model.bc_I)
  }

  /** Compute the errors between labels and predictions */
  private def computeErrors(points: RDD[SVDFeaturePoint],
                            model: SVDFeatureSharedModel): RDD[Double] = {
    val errFun = if (is_regression) {
      (t: Double, y: Double) => t - y
    } else {
      (t: Double, y: Double) => -t / (1 + math.exp(-t * y))
    }

    model.predictWithLabel(points).map {
      case (t, y) => errFun(t, y)
    }
  }

  /** Update the bias and weights for global feature */
  private def updateUser(points: RDD[SVDFeaturePoint],
                         model: SVDFeatureSharedModel,
                         lrate: Double): SVDFeatureSharedModel = {

    val es = computeErrors(points, model)

    val (t_Fu, t_K) = (model.Fu, K)
    val bc_I = model.bc_I
    val (grad_wu, grad_U) = points.zip(es).mapPartitions { pes =>
      // local gradient of wu
      val g_wu = DenseVector.zeros[Double](t_Fu)
      // local gradient of U
      val g_U = DenseMatrix.zeros[Double](t_K, t_Fu)
      // compute and collect the gradient of wu and U
      pes.foreach {
        case (pnt, e) =>
          val ifactor: DenseVector[Double] = bc_I.value dot pnt.i_fs
          pnt.u_fs.foreachActive {
            case (f, v) =>
              g_wu(f) += v * e
              for (k <- 0 until t_K)
                g_U(k, f) += v * e * ifactor(k)
          }
      }
      Iterator((g_wu, g_U))
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    // update wu and U
    val sc = points.context
    val new_wu = model.wu * (1 - lrate * reg) + grad_wu * lrate
    val bc_wu = sc.broadcast(new_wu)
    val new_U = model.U * (1 - lrate * reg) + grad_U * lrate
    val bc_U = sc.broadcast(new_U)
    new SVDFeatureSharedModel(sc, is_regression,
      model.b, bc_wu, model.bc_wi, model.bc_wg, bc_U, model.bc_I)
  }

  /** Update the bias and weights for global feature */
  private def updateItem(points: RDD[SVDFeaturePoint],
                         model: SVDFeatureSharedModel,
                         lrate: Double): SVDFeatureSharedModel = {
    val es = computeErrors(points, model)

    val (t_Fi, t_K) = (model.Fi, K)
    val bc_U = model.bc_U
    val (grad_wi, grad_I) = points.zip(es).mapPartitions { pes =>
      // local gradient of wi
      val g_wi = DenseVector.zeros[Double](t_Fi)
      // local gradient of I
      val g_I = DenseMatrix.zeros[Double](t_K, t_Fi)
      // compute and collect the gradient of wi and I
      pes.foreach {
        case (pnt, e) =>
          val ifactor: DenseVector[Double] = bc_U.value dot pnt.u_fs
          pnt.i_fs.foreachActive {
            case (f, v) =>
              g_wi(f) += v * e
              for (k <- 0 until t_K)
                g_I(k, f) += v * e * ifactor(k)
          }
      }
      Iterator((g_wi, g_I))
    }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    // update the parameters
    val new_wi = model.wi * (1 - lrate * reg) + grad_wi * lrate
    val new_I = model.I * (1 - lrate * reg) + grad_I * lrate

    val sc = points.context
    val bc_wi = sc.broadcast(new_wi)
    val bc_I = sc.broadcast(new_I)
    new SVDFeatureSharedModel(sc, is_regression,
      model.b, model.bc_wu, bc_wi, model.bc_wg, model.bc_U, bc_I)
  }

  /** Evaluate model over input data */
  private def evaluate(ps: RDD[SVDFeaturePoint],
                       model: SVDFeatureSharedModel): Double = {
    if (is_regression) {
      val tys = model.predictWithLabel(ps)
      RMSE(tys)
    } else {
      val tys = model.predictWithLabel(ps)
      accuracy(tys)
    }
  }
}
