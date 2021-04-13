package bda.spark.model.factorizationMachine

import bda.common.Logging
import bda.common.linalg.immutable.SparseVector
import bda.common.linalg.{DenseMatrix, DenseVector}
import bda.common.obj.LabeledPoint
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Classification._
import bda.spark.evaluate.Regression._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

private[factorizationMachine]
class FMSharedModel(val is_regression: Boolean,
                    val factor_matrix: DenseMatrix[Double],
                    val weight_vector: Option[DenseVector[Double]],
                    val bias: Double,
                    val min: Double,
                    val max: Double) extends FMModel with Serializable {

  /** The Number of features. */
  val num_features = factor_matrix.colNum

  /** The size of factor. */
  val num_factors = factor_matrix.rowNum

  require(num_factors > 0 && num_features > 0)
  //User for predict untrained features.
  private var mean_factor: DenseVector[Double] = DenseVector.zeros(num_factors)
  for (i <- 0 until num_features) {
    mean_factor += factor_matrix.col(i)
  }
  mean_factor = mean_factor / num_features
  private val mean_weight: Double = if (weight_vector.isDefined) weight_vector.get.sum / num_features else 0.0

  /** Predict a valid point, return the prediction. */
  private def predict(valid: SparseVector[Double]): Double = {
    var prediction = bias
    if (weight_vector.isDefined) {
      valid.foreachActive {
        case (i, v: Double) =>
          if (i < num_features)
            prediction += weight_vector.get(i) * v
          else
            prediction += mean_weight * v
      }
    }
    for (f <- 0 until num_factors) {
      var sum = 0.0
      var sum_sqr = 0.0
      valid.foreachActive {
        case (i, v) =>
          if (i < num_features) {
            val d = factor_matrix(f, i) * v
            sum += d
            sum_sqr += d * d
          }
          else {
            val d = mean_factor(f) * v
            sum += d
            sum_sqr += d * d
          }
      }
      prediction += (sum * sum - sum_sqr) / 2
    }
    if (is_regression)
      math.min(math.max(min, prediction), max)
    else if (prediction > 0) 1.0 else -1.0
  }

  /** Predict the valid data, return  (label, predict)*/
  override def predict(t: LabeledPoint): Double = predict(t.fs)


  /** Predict the valid data, return a RDD in the format RDD[(label, predict)] */
  override def predict(valid: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    valid.map {
      t =>
        (t.label, predict(t.fs))
    }
  }

  /** Save the model into model_pt,
    * which will output three files: model.matrix, model.vector, model.other.
    */
  override def saveModel(sc: SparkContext, model_pt: String): Unit = {
    sc.makeRDD(Seq(weight_vector)).saveAsObjectFile(model_pt + "model.vector")
    sc.makeRDD(Seq(factor_matrix)).saveAsObjectFile(model_pt + "model.matrix")

    val other = sc.makeRDD(Seq(is_regression.toString, bias.toString, min.toString, max.toString))
    other.saveAsObjectFile(model_pt + "model.other")
  }
}

private[factorizationMachine]
object FMSharedModel extends Logging {
  def loadModel(sc: SparkContext,
                model_pt: String): FMModel = {
    logInfo("Load model from: " + model_pt)
    val other_pt = model_pt + "model.other"
    val m_pt = model_pt + "model.matrix"
    val v_pt = model_pt + "model.vector"

    val m = sc.objectFile[DenseMatrix[Double]](m_pt).first()
    val v = sc.objectFile[Option[DenseVector[Double]]](v_pt).first()
    val Seq(is_regression, bias, min, max) = sc.objectFile[String](other_pt).collect().toSeq
    new FMSharedModel(is_regression.toBoolean, m, v, bias.toDouble, min.toDouble, max.toDouble)
  }
}

/**
 * Trainer class for spark factorization machine.
 */
private[factorizationMachine]
class FMSharedTrainer(val is_regression: Boolean,
                      val step_size: Double,
                      val max_iter: Int,
                      val dim: (Boolean, Int),
                      val reg: (Double, Double),
                      val init_std: Double = 0.1
                       ) extends FMTrainer with Serializable {

  private val k1: Boolean = dim._1
  private val k2: Int = dim._2

  private val r1: Double = reg._1
  private val r2: Double = reg._2

  private var num_features: Int = -1

  private var min_label: Double = Double.MinValue
  private var max_label: Double = Double.MaxValue

  private var bias: Double = 0.0


  /**
   * Generate initial weight of FM.
 *
   * @return Initialized weight vector
   */
  private def initWeight(): DenseVector[Double] = {
    require(num_features > 0)
    val weight = k1 match {
      case true =>
        DenseVector.randGaussian(num_features * (k2 + 1), init_std)
      case false =>
        DenseVector.randGaussian(num_features * k2, init_std)
    }
    weight
  }

  /**
   * Training a FMModel over `train_data`.
   * If `valide_data` is provided, validate the trained model during training.
   */
  override def train(train_point: RDD[LabeledPoint],
                     valid_point: RDD[LabeledPoint] = null): FMModel = {
    val loss_history = new ArrayBuffer[Double](max_iter)
    val num_example = train_point.count()
    require(num_example > 0)

    initialize(train_point)
    var weight: DenseVector[Double] = initWeight()

    val hist_time = new ArrayBuffer[Double]()
    for (iter <- 1 until max_iter + 1) {
      val time = new Timer()

      val grad: RDD[(DenseVector[Double])] = train_point.map {
        a =>
          getGradient(a.fs, weight, a.label)
      }.cache()
      val gradient_sum: DenseVector[Double] = grad.reduce(_ + _)
      //grad.unpersist(blocking = false)
      weight = updateWeight(weight, gradient_sum, iter)

      val cost_time = time.cost()
      hist_time.append(cost_time)
      val msg = Msg("iter" -> iter, "time(ms)" -> cost_time)

      val model = toModel(weight)
      if (is_regression) {
        val train_rmse = RMSE(model.predict(train_point))
        msg.append("Train_RMSE", train_rmse)

        if (valid_point != null) {
          val valid_rmse = RMSE(model.predict(valid_point))
          msg.append("Valid_RMSE", valid_rmse)
        }
        loss_history.append(train_rmse)
      }
      else {
        val train_pre: RDD[(Double, Double)] = model.predict(train_point)
        val train_acc = accuracy(train_pre)
        val train_precision = precision(train_pre)
        val train_recall = recall(train_pre)

        msg.append("Train_accuracy", train_acc)
        msg.append("Train_precision", train_precision)
        msg.append("Train_recall", train_recall)
        loss_history.append(train_acc)
        if (valid_point != null) {
          val valid_pre: RDD[(Double, Double)] = model.predict(valid_point)
          val valid_acc = accuracy(valid_pre)
          val valid_precision = precision(valid_pre)
          val valid_recall = recall(valid_pre)

          msg.append("Valid_accuracy", valid_acc)
          msg.append("Valid_precision", valid_precision)
          msg.append("Valid_recall", valid_recall)
        }
      }

      logInfo(msg.toString)
    }
    logInfo(s"Gradient descent finished, all rmse is %s.".format(loss_history.mkString(", ")))
    logInfo("average iteration time:" + hist_time.sum / hist_time.size + "ms")
    toModel(weight)
  }

  /**
   * Build a FMModel with weight vector
 *
   * @param weight weight vecotr of FM model, include factor matrix and weight vector.
   * @return FMModel
   */
  def toModel(weight: DenseVector[Double]): FMModel = {
    val t = new ArrayBuffer[Array[Double]]()
    for (i <- 0 until k2) {
      val temp = new ArrayBuffer[Double]()
      for (j <- 0 until num_features) {
        temp.append(weight(j * k2 + i))
      }
      t.append(temp.toArray)
    }
    //2-way interaction weight, stored in a matrix
    val v = new DenseMatrix[Double](t.toArray)
    //1-way interaction weight, stored in a Densevector
    val w_1 = Array.fill[Double](num_features)(0.0)
    for (i <- 0 until num_features)
      w_1(i) = weight(num_features * k2 + i)
    val w: Option[DenseVector[Double]] = if (k1) Some(new DenseVector[Double](w_1)) else None
    //return a FM model.
    new FMSharedModel(is_regression, v, w, bias, min_label, max_label)
  }

  /**
   * Initialize some value like num_features、bias with train_data.
   * In regression, min_label and max_label will be set.
 *
   * @param train_point train_data in the format (label, feature[SparseVector])
   */
  private def initialize(train_point: RDD[LabeledPoint]): Unit = {
    if (train_point.getStorageLevel == StorageLevel.NONE) {
      logWarn("This input data is not directly cached, which may hurt performance if its parents RDDs are also uncached. ")
    }
    this.num_features = train_point.map(a => a.fs.maxActiveIndex).max + 1
    require(num_features > 0)
    if (is_regression) {
      val scores = train_point.map(t => t.label).cache()
      bias = scores.sum * 1.0 / scores.count()
      scores.unpersist(blocking = false)
    }
    else {
      val pos = train_point.filter(t => t.label > 0).count()
      val all = train_point.count()
      bias = (2 * pos - all) * 1.0 / all
    }

    /**
     * Regression task, we need to know the max and min of these labels.
     */
    if (is_regression) {
      val (minT, maxT) = train_point.map(_.label).aggregate[(Double, Double)]((Double.MaxValue, Double.MinValue))({
        //map
        case ((min, max), v) =>
          (Math.min(min, v), Math.max(max, v))
      }, {
        //reduce
        case ((min1, max1), (min2, max2)) =>
          (Math.min(min1, min2), Math.max(max1, max2))
      })
      this.min_label = minT
      this.max_label = maxT
    }
  }


  /**
   * Update weight of FM with gradient.
 *
   * @param iter  The count of iter times, default: iter = 1
   *              It can help modify the learn_rate, because learn_rate should become smaller with iteration.
   * @param gradient  Sum Gradient of one iteration. A DenseVector.
   * @return New_weight.
   */
  private def updateWeight(weight: DenseVector[Double], gradient: DenseVector[Double],
                           iter: Int = 1): DenseVector[Double] = {
    val this_step_size = step_size / math.sqrt(iter)
    val len = weight.size
    val new_weight = Array.fill(len)(0.0)
    if (k1) {
      for (i <- num_features * k2 until num_features * k2 + num_features) {
        new_weight(i) = weight(i) * (1 - this_step_size * r1) - this_step_size * gradient(i)
      }
    }

    for (i <- 0 until num_features * k2) {
      new_weight(i) = weight(i) * (1 - this_step_size * r2) - this_step_size * gradient(i)
    }
    // return the new weight and the regularization.
    new DenseVector[Double](new_weight)
  }

  /**
   * This Func will in the map phase.
 *
   * @param data  A sample in the format of sparseVector
   * @param weights  The FM weight
   * @param label  Label of the sample
   * @return  Gradient of this sample
   */
  private def getGradient(data: SparseVector[Double], weights: DenseVector[Double], label: Double
                           ): DenseVector[Double] = {
    val (pred, sum) = predict(data, weights)
    val mult = if (is_regression)
      pred - label
    else
      -label / (1.0 + math.exp(label * pred))

    val grad = Array.fill(weights.size)(0.0)
    if (k1) {
      val pos = num_features * k2
      data.foreachActive({
        case (i, v) =>
          grad(pos + i) = v * mult
      })
    }
    data.foreachActive({
      case (i, v) =>
        val pos = i * k2
        for (f <- 0 until k2) {
          grad(pos + f) = (sum(f) * v - weights(pos + f) * v * v) * mult
        }
    })
    new DenseVector[Double](grad)
  }

  /**
   * Will parallelize
   * Predict the label of a sample. Will be called in Func getGradient.
 *
   * @param data  A sample in the format of sparseVector
   * @param weight The FM weight
   * @return  Predicted Label
   */
  private def predict(data: SparseVector[Double],
                      weight: DenseVector[Double]): (Double, Array[Double]) = {
    var pred = bias
    if (k1) {
      val pos = num_features * k2
      data.foreachActive({
        case (i, v) =>
          pred += weight(pos + i) * v
      })
    }
    val sum = Array.fill(k2)(0.0)
    for (f <- 0 until k2) {
      var sum_sqr = 0.0
      data.foreachActive({
        case (i, v) =>
          val d = weight(i * k2 + f) * v
          sum(f) += d
          sum_sqr += d * d
      })
      pred += (sum(f) * sum(f) - sum_sqr) / 2
    }
    if (is_regression)
      pred = math.min(math.max(pred, min_label), max_label)
    else {
      if (pred > 0) 1.0 else -1.0
    }
    (pred, sum)
  }
}

