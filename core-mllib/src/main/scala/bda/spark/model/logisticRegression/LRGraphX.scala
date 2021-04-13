package bda.spark.model.logisticRegression

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.LabeledPoint
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Classification.accuracy
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * Logistic regression model for GraphX
  * @param bs  bias for each class, C-dim DenseVector
  * @param W  Weights of features, each entry is (fid, C-dimension vector)
  */
private[logisticRegression]
class LRGraphXModel(val bs: DenseVector[Double],
                    val W: RDD[(Int, DenseVector[Double])])
  extends LRModel with Logging {

  if (W.getStorageLevel == StorageLevel.NONE) W.cache()

  val class_num = bs.size

  /** The last entry of W is the bias */
  val feature_num: Int = W.count().toInt

  /** Evaluate P(y|p) of each class for a point */
  protected def evalY(p: LabeledPoint): DenseVector[Double] =
    W.context.parallelize(p.fs.active).join(W).map {
      case (fid, (v, ws)) => ws * v
    }.treeReduce(_ + _) + bs

  /** Evaluate P(y|p) of each class for a point collection */
  protected def evalY(points: RDD[LabeledPoint]): RDD[DenseVector[Double]] = {
    val f_dvs = points.zipWithIndex().flatMap {
      case (pnt, pid) => pnt.fs.active.map {
        case (fid, v) => (fid, (pid, v))
      }
    }.groupByKey()
    // evaluate ys for each point
    val p_ys = f_dvs.join(W).flatMap {
      case (fid, (dvs, ws)) => dvs.map {
        case (pid, v) => (pid, ws * v)
      }
    }.foldByKey(bs)(_ + _)
    p_ys.map {
      case (pid, ys) => (pid, ys)
    }.sortBy(_._1).map(_._2)
  }

  /**
    * Save the model
    * @param model_pt  A local or HDFS path
    */
  def save(model_pt: String): Unit = {
    logInfo("save model to:" + model_pt)
    val sc = W.context
    val bs_rdd = sc.makeRDD(Seq(bs))
    bs_rdd.saveAsObjectFile(model_pt + "/model.bs")
    W.saveAsObjectFile(model_pt + "/model.W")
  }
}


/** Factory methods to create a Model */
private[logisticRegression] object LRGraphXModel extends Logging {

  /** Random initialize a mode */
  def rand(sc: SparkContext,
           class_num: Int,
           feature_num: Int): LRGraphXModel = {

    val bs = DenseVector.zeros[Double](class_num)
    //define the feature weights as target vertices
    val W: RDD[(Int, DenseVector[Double])] =
      sc.parallelize(0 until feature_num).map { fid =>
        (fid, DenseVector.rand(class_num) * 0.01)
      }
    new LRGraphXModel(bs, W)
  }

  /**
    * Load a model from file
    * @param model_dir  The directory should contain files: model.bs and model.W
    * @return A LRGraphX model
    */
  def load(sc: SparkContext, model_dir: String): LRGraphXModel = {
    logInfo(s"load model from: $model_dir")
    val bs = sc.objectFile[DenseVector[Double]](model_dir + "/model.bs").first()
    val W = sc.objectFile[(Int, DenseVector[Double])](model_dir + "/model.W")
    new LRGraphXModel(bs, W)
  }
}

/** Base vertex attribute for defining a bipartite graph */
private[logisticRegression] class VertexAttr()

/** Vertex attribute for data point with a label */
private[logisticRegression]
case class DataAttr(label: Int) extends VertexAttr

/** Vertex attribute class for feature weights */
private[logisticRegression]
case class ParamAttr(vs: DenseVector[Double]) extends VertexAttr

/**
  * Bipartite graph with two types of nodes.
  *
  * - data vertices with negative ids
  * - parameter vertices with positive or zero ids
  * @param bs  bias vector. A global constant for the graph.
  * @param graph  sample-weight graph
  */
private[logisticRegression] class LRGraph(val bs: DenseVector[Double],
                                          val graph: Graph[VertexAttr, Double]) {

  /** Extract data vertices from the bipartite graph */
  def dataVertices: RDD[(VertexId, VertexAttr)] = graph.vertices.filter {
    case (id, vertex) => id < 0
  }

  /** Extract feature vertices from the bipartite graph */
  def paramVertices: RDD[(VertexId, VertexAttr)] = graph.vertices.filter {
    case (id, vertex) => id >= 0
  }

  lazy val numVertices = graph.numVertices
  lazy val numEdges = graph.numEdges
  lazy val numDataVertices = dataVertices.count()
  lazy val numParamVertices = paramVertices.count()

  /** Generate a [[LRGraphXModel]] from the graph */
  def toModel: LRGraphXModel = new LRGraphXModel(bs, weights)

  /** Extract feature weights from the bipartite graph */
  private def weights: RDD[(Int, DenseVector[Double])] = paramVertices.map {
    case (id, vertex) =>
      val ws = vertex match {
        case p: ParamAttr => p.vs
        case other =>
          throw new IllegalArgumentException(s"$other is not a type of ParamAttr.")
      }
      (id.toInt, ws)
  }

  /** Replace the label of each data vertex by the error vector */
  def gradientDescentUpdate(lrate: Double, reg: Double): LRGraph = {
    val ys: VertexRDD[DenseVector[Double]] = evalClassProbability()
    val err_graph = graph.joinVertices(ys) {
      case (id, pn, ys) =>
        // get the label of data point
        val t = pn match {
          case da: DataAttr => da.label
          case other =>
            throw new RuntimeException(s"cannot convert to DataAttr from $other")
        }
        val es: DenseVector[Double] = -ys
        es(t) += 1
        ParamAttr(es)
    }
    // update bias
    val grad_bs: DenseVector[Double] = err_graph.vertices.filter(_._1 < 0).map {
      case (id, attr) => attr.asInstanceOf[ParamAttr].vs
    }.treeReduce(_ + _)
    val new_bs = bs * (1 - lrate * reg) + grad_bs * lrate

    // update weights
    val grad_ws: VertexRDD[DenseVector[Double]] =
      err_graph.aggregateMessages[DenseVector[Double]](
        // triplet: (arcAttr: ParamAttr, attr: Double, dstAttr: ParamAttr)
        tri => tri.sendToDst(tri.srcAttr.asInstanceOf[ParamAttr].vs * tri.attr),
        (a, b) => a + b
      )
    val new_graph = graph.joinVertices(grad_ws) {
      case (id, attr, g_ws) =>
        val ws = attr.asInstanceOf[ParamAttr].vs
        new ParamAttr(ws * (1 - lrate * reg) + g_ws * lrate)
    }
    new LRGraph(new_bs, new_graph)
  }

  /** Generate the predictions for each data point */
  private def evalY(): VertexRDD[DenseVector[Double]] = {
    val tmp_bs = bs
    graph.aggregateMessages[DenseVector[Double]](
      tri =>
        tri.sendToSrc(tri.dstAttr.asInstanceOf[ParamAttr].vs * tri.attr),
      (a, b) => a + b
    ).mapValues(_ + tmp_bs)
  }

  /** Evaluate the probability of data vertices belong to each class */
  def evalClassProbability(): VertexRDD[DenseVector[Double]] =
    evalY().mapValues(_.expNormalize())

  /** Predict the label for each data point */
  def predict(): VertexRDD[Int] = evalY().mapValues(_.argmax)

  /** Create a new graph by copy the parameters of that graph */
  def replaceParams(that: LRGraph): LRGraph = {
    val new_graph = graph.joinVertices(that.paramVertices) {
      case (id, ws1, ws2) => ws2
    }
    new LRGraph(that.bs, new_graph)
  }

  /** Create a new graph by replacing the data vertices in
    * current graph to `points`.
    */
  def replaceData(points: RDD[LabeledPoint]): LRGraph = {
    val pnts = LRGraph.indexPoints(points)
    val data_vts = LRGraph.createDataVertices(pnts)
    val vts = data_vts.union(this.paramVertices)
    val edges = LRGraph.createEdges(pnts)
    val g = Graph(vts, edges)
    new LRGraph(bs, g)
  }

  /** Evaluate the accuracy of `model` over data `points` */
  def evaluate(): Double = {
    val ys: RDD[(VertexId, Int)] = predict()
    val tys = dataVertices.join(ys).map {
      case (id, (attr, y)) =>
        (attr.asInstanceOf[DataAttr].label.toDouble, y.toDouble)
    }
    accuracy(tys)
  }

  /** Cache the graph */
  def cache(): this.type = {
    graph.cache()
    this
  }

  /** materialize the graph */
  def materialize(): this.type = {
    graph.edges.foreachPartition(_ => Unit)
    this
  }

  /** Unpersist the graph */
  def unpersist(): this.type = {
    graph.unpersist()
    this
  }
}

/** Factory of bipartite graph for logistic regression */
private[logisticRegression] object LRGraph {

  /** Attach a negative id to each point */
  def indexPoints(points: RDD[LabeledPoint]): RDD[(Long, LabeledPoint)] =
    points.zipWithIndex().map {
      case (pnt, id) => (-id - 1, pnt)
    }

  /** Transform data points into vertices (with negative nodeIds) in the bipartite graph */
  def createDataVertices(points: RDD[(Long, LabeledPoint)]):
  RDD[(VertexId, VertexAttr)] =
    points.map {
      case (id, pn) => (id, DataAttr(math.round(pn.label).toInt))
    }

  /** Transform feature weights into vertices in the bipartite graph */
  def createParamVertices(weights: RDD[(Int, DenseVector[Double])]):
  RDD[(VertexId, VertexAttr)] = weights.map {
    case (fid, ws) => (fid.toLong, ParamAttr(ws))
  }

  /** Create edges from points. The edge weight is the feature value in a point. */
  def createEdges(points: RDD[(Long, LabeledPoint)]): RDD[Edge[Double]] = points.flatMap {
    case (id, pn) => pn.fs.active.map {
      case (fid, v) => Edge(id, fid, v)
    }
  }

  /**
    * Construct a sample-parameter bipartite graph from  unlabeled points RDD.
    *
    * The bipartite graph contains:
    * - data vertex: the points with negative ids,
    * - parameter vertex: the weights of features
    * - edges: the value of non-zero feature in each point
    * @return  A bipartite graph, from data points to feature weights
    */
  def apply(points: RDD[LabeledPoint], model: LRGraphXModel): LRGraph = {
    val pnts = indexPoints(points).cache()
    val data_vts = createDataVertices(pnts)
    val weight_vts = createParamVertices(model.W)
    val vts = data_vts.union(weight_vts)
    val edges = createEdges(pnts)
    val graph = Graph(vts, edges)
    new LRGraph(model.bs, graph)
  }
}

/** Training logistic regression on GraphX */
private[logisticRegression]
class LRGraphXTrainer(val class_num: Int,
                      val feature_num: Int,
                      val max_iter: Int,
                      val learn_rate: Double,
                      val reg: Double) extends LRTrainer {

  /**
    * Training a LRModel over `train_points`.
    * If `validate_points` is provided, validate the trained model during training.
    */
  def train(train_points: RDD[LabeledPoint],
            valid_points: RDD[LabeledPoint] = null): LRGraphXModel = {
    val sc = train_points.sparkContext
    val model = LRGraphXModel.rand(sc, class_num, feature_num)
    // build a bipartite graph for training points
    var train_graph: LRGraph = LRGraph(train_points, model).cache().materialize()
    train_points.unpersist()

    // build a bipartite graph for validation points. The parameter
    // vertices in train_graph are reused.
    var valid_graph: LRGraph = if (valid_points == null) null
    else {
      train_graph.replaceData(valid_points).cache()
    }

    if (valid_points != null) {
      valid_graph.materialize()
      valid_points.unpersist()
    }

    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      val pre_g = train_graph
      // update the LRGraph for training points
      val lrate = learn_rate / math.sqrt(iter)
      train_graph = train_graph.gradientDescentUpdate(lrate, reg).cache().materialize()

      // update the LRGraph for validation points
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
      val msg = Msg("iter" -> iter,
        "time cost" -> time_cost,
        "accuracy(train)" -> train_graph.evaluate())
      if (valid_graph != null)
        msg.append("accuracy(validate)", valid_graph.evaluate())

      logInfo(msg.toString)
    }

    train_graph.toModel
  }

}