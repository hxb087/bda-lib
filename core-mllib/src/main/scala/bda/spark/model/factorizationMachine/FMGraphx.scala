package bda.spark.model.factorizationMachine

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.linalg.immutable.SparseVector
import bda.common.obj.LabeledPoint
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Classification.accuracy
import bda.spark.evaluate.Regression.RMSE
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.util.Random

class FMGraphxModel(val is_regression: Boolean,
                    val bias: Double,
                    val weight: RDD[(VertexId, (DenseVector[Double], Double))],
                    val min: Double,
                    val max: Double) extends FMModel with Serializable {

  private val num_features: Int = weight.first()._2._1.size

  /** Predict test data and return a [label, prediction] RDD. */
  override def predict(test: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    test.map {
      da =>
        //label, predict.
        (da.label, predict(da.fs))
    }
  }

  /** Predict test data and return a [label, prediction] pair. */
  override def predict(test: LabeledPoint): Double = predict(test.fs)


  /** Predict a test data sample. O(kn) */
  private def predict(test: SparseVector[Double]): Double = {
    var sum: DenseVector[Double] = DenseVector.zeros(num_features)
    var sum_square: Double = 0.0
    var predict: Double = bias
    val a = weight.context.parallelize(test.active)
      .map(a => (a._1.toLong, a._2))
      .join(weight).map {
      case (fid, (v, (v2, w1))) =>
        sum += v2 * v
        sum_square += (v2 * v).dot(v2 * v)
        predict += v * w1
    }
    predict += (sum.dot(sum) - sum_square) / 2
    if (is_regression) {
      predict = math.min(math.max(predict, min), max)
    }
    else {
      if (predict > 0) predict = 1.0 else -1.0
    }
    predict
  }

  /** Save a model into a file. */
  override def saveModel(sc: SparkContext, model_pt: String): Unit = {
    weight.saveAsObjectFile(model_pt + ".weight")
    val other = sc.makeRDD(Array(is_regression, bias, min, max))
    other.saveAsObjectFile(model_pt + ".other")
  }
}

object FMGraphxModel extends Logging {

  /**
   * Load a model from file
   * @param model_pt The directory should contain files: model.weight and model.other
   * @return A FMGraphX model
   */
  def loadModel(sc: SparkContext, model_pt: String): FMGraphxModel = {
    logInfo(s"load model from: $model_pt")
    val weight_pt = model_pt + ".weight"
    val weight: RDD[(VertexId, (DenseVector[Double], Double))] = sc.objectFile[(VertexId, (DenseVector[Double], Double))](weight_pt)
    val other_pt = model_pt + ".other"
    val vs: Array[Any] = sc.objectFile[Any](other_pt).collect()
    val is_regression = vs(0).asInstanceOf[Boolean]
    val bias = vs(1).asInstanceOf[Double]
    val min = vs(2).asInstanceOf[Double]
    val max = vs(3).asInstanceOf[Double]
    new FMGraphxModel(is_regression, bias, weight, min, max)
  }

  /** Random initialize o model. */
  def randModel(sc: SparkContext,
                feature_num: Int,
                dim: (Boolean, Int),
                init_std: Double,
                is_regression: Boolean): FMGraphxModel = {
    val k2 = dim._2
    val W: RDD[(Long, (DenseVector[Double], Double))] = sc.parallelize(0 until feature_num).map {
      fid =>
        (fid.toLong, (DenseVector.randGaussian(k2, init_std), Random.nextDouble))
    }
    new FMGraphxModel(is_regression, 0.0, W, Double.MinValue, Double.MaxValue)
  }
}

/** Base vertex attribute for defining a bipartite graph */
private[factorizationMachine] class VertexAttr()

/** Vertex attribute for data  with a label */
private[factorizationMachine]
case class DataAttr(label: Double) extends VertexAttr

/** Vertex attribute class for feature weights */
private[factorizationMachine]
case class ParamAttr(vs: (DenseVector[Double], Double)) extends VertexAttr

private[factorizationMachine] class FMGraph(val is_regression: Boolean,
                                            val graph: Graph[VertexAttr, Double])
  extends Serializable {

  private var bias: Double = 0.0
  private var min: Double = Double.MinValue
  private var max: Double = Double.MaxValue

  /** Extract data vertices from the bipartite graph */
  def dataVertices: RDD[(VertexId, VertexAttr)] = graph.vertices.filter {
    case (id, v_attr) => id < 0
  }

  /** Extract features vertices from the bipartite graph */
  def paramVertices: RDD[(VertexId, VertexAttr)] = graph.vertices.filter {
    case (id, v_attr) => id >= 0
  }

  lazy val numVertices = graph.numVertices
  lazy val numEdges = graph.numEdges
  lazy val numDataVertices = dataVertices.count()
  lazy val numParamVertices = paramVertices.count()

  /** Get Bias Min Max of the model. */
  def getBiasMinMax = {
    if (is_regression) {
      //regression
      val label = dataVertices.map {
        case (id, l) =>
          val ws = l match {
            case p: DataAttr => p.label
          }
          ws
      }.cache()
      bias = label.sum() / numDataVertices
      min = label.min()
      max = label.max()
    }
    else {
      //classification
      val pos = dataVertices.map {
        case (id, l) =>
          val ws = l match {
            case p: DataAttr => p.label
          }
          if (ws > 0) 1 else 0
      }.sum()
      bias = (2 * pos - numDataVertices) * 1.0 / numDataVertices
    }
  }

  /** Extract feature weights from the bipartite graph */
  private def weights: RDD[(VertexId, (DenseVector[Double], Double))] = paramVertices.map {
    case (id, vertex) =>
      val ws = vertex match {
        case p: ParamAttr => p.vs
        case other =>
          throw new IllegalArgumentException(s"$other is not a type of ParamAttr.")
      }
      (id, ws)
  }

  /** Generate a [[FMGraphxModel]] from the graph */
  def toModel: FMGraphxModel = {
    new FMGraphxModel(is_regression, bias, weights, min, max)
  }

  /** Gradient descent update the Param Vertices. */
  def gradientDesecntUpdate(learn_rate: Double,
                            dim: (Boolean, Int) = (true, 8),
                            reg: (Double, Double),
                            iter: Int = 1): FMGraph = {
    val k1 = dim._1
    val k2 = dim._2

    val this_learn_rate = learn_rate / math.sqrt(iter)
    //gradient.
    val factor_pre: RDD[(VertexId, (DenseVector[Double], Double))] = predict().cache()
    // Calculate y(x) - label.
    val err_graph: Graph[VertexAttr, Double] = graph.joinVertices(factor_pre) {
      case (id, label, (factor, pre)) =>
        val la = label.asInstanceOf[DataAttr].label
        var error = 0.0
        if (is_regression) {
          error = pre - la
        }
        else {
          error = -la / (1.0 + math.exp(la * pre))
        }
        ParamAttr((factor, error))
    }

    //update weight
    val grad_ws: VertexRDD[(DenseVector[Double], Double)] = err_graph
      .aggregateMessages[(DenseVector[Double], Double)](
        tri => {
          val error = tri.srcAttr.asInstanceOf[ParamAttr].vs._2
          val m = tri.srcAttr.asInstanceOf[ParamAttr].vs._1 * tri.attr
          -tri.dstAttr.asInstanceOf[ParamAttr].vs._1 * (tri.attr * tri.attr)
          val m_g = m * error
          val v_g = error * tri.attr
          tri.sendToDst((m_g, v_g))
        },
        (a, b) => (a._1 + b._1, a._2 + b._2)
      )
    val new_graph = graph.joinVertices(grad_ws) {
      case (id, attr, g_ws) =>
        val ws = attr.asInstanceOf[ParamAttr].vs
        val t2 = 1 - this_learn_rate * reg._2
        if (k1) {
          val t1 = 1 - this_learn_rate * reg._1
          new ParamAttr((ws._1 * t2 - g_ws._1 * this_learn_rate, ws._2 * t1 - g_ws._2 * this_learn_rate))
        }
        else {
          new ParamAttr((ws._1 * t2 - g_ws._1 * this_learn_rate, 0.0))
        }
    }

    new_graph.cache()

    val fm_graph = new FMGraph(is_regression, new_graph)
    fm_graph.getBiasMinMax
    fm_graph
  }

  /** Predict the Label of dataVertices. */
  private def predict(): RDD[(VertexId, (DenseVector[Double], Double))] = {
    //sum_j=0^j=N (V_j * x_j)
    val factor_sum: VertexRDD[DenseVector[Double]] = graph.aggregateMessages[DenseVector[Double]](
      // triplet: srcAttr: label, attr: Double, dstAttr: (m, v)
      tri =>
        tri.sendToSrc(tri.dstAttr.asInstanceOf[ParamAttr].vs._1 * tri.attr),
      (a, b) => a + b
    )

    //sum_j=0^j=N (V_j * x_j) (V_j * x_j)
    //w_i * x_i
    val t: VertexRDD[(Double, Double)] = graph.aggregateMessages[(Double, Double)](
      tri => {
        val factor = tri.dstAttr.asInstanceOf[ParamAttr].vs._1 * tri.attr
        val factor_square = factor dot factor
        val w_x = tri.dstAttr.asInstanceOf[ParamAttr].vs._2 * tri.attr
        tri.sendToSrc((factor_square, w_x))
      },
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )

    //predict
    val factor_pre: RDD[(VertexId, (DenseVector[Double], Double))] = factor_sum.join(t).map {
      case (id, (v, (t, w))) =>
        var pre = bias + (v.dot(v) - t) / 2 + w
        if (is_regression) {
          pre = math.min(math.max(pre, min), max)
        }
        else {
          if (pre > 0) pre = 1.0 else pre = -1.0
        }
        (id, (v, pre))
    }
    //factor_sum can be used in gradient calculation.
    factor_pre
  }

  /**
   * Evaluate the Algorithm on training data.
   * @return RMSE or accuracy.
   */
  def evaluate(): Double = {
    val factor_pre: RDD[(VertexId, (DenseVector[Double], Double))] = predict()
    val label_pre = factor_pre.join(dataVertices).map {
      case (id, ((factor, pre), la)) =>
        (la.asInstanceOf[DataAttr].label, pre)
    }
    if (is_regression) {
      RMSE(label_pre)
    }
    else {
      val pair = label_pre.map(a => (a._1, a._2))
      accuracy(pair)
    }
  }

  /** Create a new graph by replacing the data vertices in
    * current graph to `data_vertices`.
    */
  def replaceData(data: RDD[LabeledPoint], is_regression: Boolean): FMGraph = {
    val pnts = FMGraph.indexData(data)
//    val feature_num = data.map(a => a.fs.maxActiveIndex).max + 1
    val data_vts: RDD[(VertexId, VertexAttr)] = FMGraph.createDataVertices(pnts)
//    val p_vts: RDD[(VertexId, VertexAttr)] = data.context
//      .parallelize(0 until feature_num).map {
//      fid =>
//        (fid.toLong, ParamAttr(DenseVector.randGaussian(2, 0.01), 0.0))
//    }
    val vts = data_vts.union(this.paramVertices)
    val edges = FMGraph.createEdges(pnts)
    val g = Graph(vts, edges)
    val fm = new FMGraph(is_regression, g)
    fm.getBiasMinMax
    fm
  }

  /** Create a new graph by copy the parameters of that graph */
  def replaceParams(that: FMGraph): FMGraph = {
    val k2: Int = that.paramVertices.first._2.asInstanceOf[ParamAttr].vs._1.size
    val new_graph = graph.outerJoinVertices(that.paramVertices) {
      case (id, ws1, ws2) =>
        if (id >= 0) {
          ws2.getOrElse(ParamAttr(DenseVector.randGaussian(k2, 0.01), 0.0))
        }
        else {
          ws1
        }
    }
    val fm = new FMGraph(that.is_regression, new_graph)
    fm.getBiasMinMax
    fm
  }

  /** Materialize the grpha */
  def materialize(): this.type = {
    //graph.vertices.foreachPartition(_ => Unit)
    graph.edges.foreachPartition(_ => Unit)
    this
  }

  /** Uncache the graph. */
  def unpersist(): this.type = {
    graph.unpersist()
    this
  }

  /** Cache the graph. */
  def cache(): this.type = {
    graph.cache()
    this
  }
}

private[factorizationMachine]
object FMGraph extends Serializable {

  /** Attach a negative id to each  */
  def indexData(da: RDD[LabeledPoint]): RDD[(VertexId, LabeledPoint)] =
    da.zipWithIndex().map {
      case (pnt, id) => (-id - 1, pnt)
    }


  /** Transform data into vertices (with negative nodeIds) in the bipartite graph */
  def createDataVertices(data: RDD[(VertexId, LabeledPoint)]): RDD[(VertexId, VertexAttr)] = {
    data.map {
      case (id, dn) => (id, DataAttr(dn.label))
    }
  }

  /** Transform feature weights into vertices in the bipartite graph */
  def createParamVertices(weights: RDD[(Long, (DenseVector[Double], Double))]): RDD[(VertexId, VertexAttr)] = {
    weights.map {
      case (fid, (v, w)) =>
        (fid, ParamAttr(v, w))
    }
  }

  /** Create edges from s. The edge weight is the feature value in a data sample. */
  def createEdges(data: RDD[(VertexId, LabeledPoint)]): RDD[Edge[Double]] =
    data.flatMap {
      case (id, pn) => pn.fs.active.map {
        case (fid, v) => Edge(id, fid, v)
      }
    }

  /**
   * Construct a sample-parameter bipartite graph from labeled s RDD.
   *
   * The bipartite graph contains:
   * - data vertex: the s with negative ids,
   * - parameter vertex: the weights of features
   * - edges: the value of non-zero feature in each sample.
   * @return  A bipartite graph, from data s to feature weights
   */
  def apply(data: RDD[LabeledPoint],
            model: FMGraphxModel, is_regression: Boolean): FMGraph = {
    val pnts = indexData(data).cache()
    val data_vts = createDataVertices(pnts)
    val weight_vts = createParamVertices(model.weight)
    val vts = data_vts.union(weight_vts)
    val edges = createEdges(pnts)
    val graph = Graph(vts, edges)
    new FMGraph(is_regression, graph)
  }
}

/** Training logistic regression on GraphX */
private[factorizationMachine]
class FMGraphxTrainer(val is_regression: Boolean,
                      val learn_rate: Double,
                      val max_iter: Int,
                      val dim: (Boolean, Int),
                      val reg: (Double, Double),
                      val init_std: Double = 0.1) extends FMTrainer {

  /**
   * Training a FMModel over `train_points`.
   * If `validate_points` is provided, validate the trained model during training.
   */
  def train(train_point: RDD[LabeledPoint],
            valid_point: RDD[LabeledPoint] = null): FMGraphxModel = {

    val sc = train_point.sparkContext
    val feature_num = train_point.map(a => a.fs.maxActiveIndex).max() + 1

    val model: FMGraphxModel = FMGraphxModel.randModel(sc, feature_num, dim, init_std, is_regression)

    // build a bipartite graph for training points
    var train_graph: FMGraph = FMGraph(train_point, model, is_regression).cache().materialize()
    val example_num = train_point.count()
    train_point.unpersist()
    train_graph.getBiasMinMax

    // build a bipartite graph for validation points. The parameter
    // vertices in train_graph are reused.
    var valid_graph: FMGraph = if (valid_point == null) null
    else {
      train_graph.replaceData(valid_point, is_regression).cache()
    }

    if (valid_point != null) {
      valid_graph.materialize()
      valid_graph.getBiasMinMax
      valid_point.unpersist()
    }

    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      val pre_g = train_graph
      // update the FMGraph for training points
      train_graph = train_graph.gradientDesecntUpdate(learn_rate, dim, reg, iter).cache().materialize()
      // update the FMGraph for validation points
      val pre_vg = valid_graph
      if (valid_graph != null) {
        valid_graph = valid_graph.replaceParams(train_graph).cache().materialize()
      }

      // free the old graphs
      pre_g.unpersist()
      if (pre_vg != null)
        pre_vg.unpersist()

      // evaluation
      val time_cost = timer.cost
      val msg = Msg("iter" -> iter, "time cost:(ms)" -> time_cost)
      if (is_regression) {
        // Regression. RMSE.
        msg.append("RMSE(train)", train_graph.evaluate())
        if (valid_graph != null)
          msg.append("RMSE(validate)", valid_graph.evaluate())
      }
      else {
        //Classification. Accuracy.
        msg.append("accuracy(train)", train_graph.evaluate())
        if (valid_graph != null)
          msg.append("accuracy(validate)", valid_graph.evaluate())
      }
      logInfo(msg.toString)
    }

    train_graph.toModel
  }
}