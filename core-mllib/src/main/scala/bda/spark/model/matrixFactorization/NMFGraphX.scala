package bda.spark.model.matrixFactorization

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.Rate
import bda.common.util.{Msg, Timer}
import bda.spark.evaluate.Regression.RMSE
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

private[matrixFactorization]
class NMFGraphXModel(val W: RDD[(Int, DenseVector[Double])],
                     val H: RDD[(Int, DenseVector[Double])])
  extends NMFModel with Logging {

  override val K = W.first()._2.size

  if (W.getStorageLevel == StorageLevel.NONE) W.cache()

  if (H.getStorageLevel == StorageLevel.NONE) H.cache()

  /** Use the mean of ws as the latent factor for missing rows */
  private val mean_ws = W.values.reduce(_ + _) / W.count()

  /** Use the mean of hs as the latent factor for missing columns */
  private val mean_hs = H.values.reduce(_ + _) / H.count()

  /**
    * Predict the value of R(i, j). If i or j not recorded in the model,
    * then use the mean value of latent factors to predict.
    */
  override def predict(rate: Rate): Double = {
    val ws_seq: Seq[DenseVector[Double]] = W.lookup(rate.user)
    val ws = if (ws_seq.nonEmpty) ws_seq.head else mean_ws

    val hs_seq: Seq[DenseVector[Double]] = H.lookup(rate.item)
    val hs = if (hs_seq.nonEmpty) hs_seq.head else mean_hs
    ws dot hs
  }

  /** Predict the values of multiple R(i, j)s */
  override def predict(rates: RDD[Rate]): RDD[(Double, Rate)] = {
    val (default_ws, default_hs) = (mean_ws, mean_hs)

    rates.map { rate =>
      (rate.user, rate)
    }.leftOuterJoin(W).map {
      case (i, (rate, Some(ws))) => (rate.item, (rate, ws))
      case (i, (rate, None)) => (rate.item, (rate, default_ws))
    }.leftOuterJoin(H).map {
      case (j, ((rate, ws), Some(hs))) => (ws dot hs, rate)
      case (j, ((rate, ws), None)) => (ws dot default_hs, rate)
    }
  }

  /** Unpersist W and H */
  def clean(): Unit = {
    W.unpersist()
    H.unpersist()
  }

  override def save(model_pt: String): Unit = {
    logInfo("save model into: " + model_pt)
    val W_pt = model_pt + "model.W"
    W.saveAsObjectFile(W_pt)
    val H_pt = model_pt + "model.H"
    H.saveAsObjectFile(H_pt)
  }
}

/** Factory of NMFGraphXModel */
object NMFGraphXModel extends Logging {

  def load(sc: SparkContext, model_pt: String): NMFGraphXModel = {
    logInfo("save model into: " + model_pt)
    val W_pt = model_pt + "model.W"
    val W = sc.objectFile[(Int, DenseVector[Double])](W_pt)
    val H_pt = model_pt + "model.H"
    val H = sc.objectFile[(Int, DenseVector[Double])](H_pt)
    new NMFGraphXModel(W, H)
  }
}

/**
  * Gradient descent algorithm for NMF implementation by GraphX
  *
  * Formally,
  * R = WH
  * Update Rule:
  * W_i <- (1-learn_rate*reg)*W_i + learn_rate \sum_j{ (d_ij - W_i*H_j) * H_j }
  * H_j <- (1-learn_rate*reg)*H_j + learn_rate \sum_i{ (d_ij - W_i*H_j) * W_i }
  *
  * R is stored in a graph with two types of nodes:
  *
  * @param learn_rate Learning rate
  * @param reg  Regularization coefficient
  */
private[matrixFactorization]
class NMFGraphXTrainer(val K: Int,
                       val learn_rate: Double,
                       val reg: Double,
                       val max_iter: Int) extends NMFTrainer with Serializable {

  /**
    * Build a graph to store the train data and parameters. The initial values of the
    * parameters are randomly initialized.
    *
    * The rowIds in `triples` are stored in even nodes; the colIds in `triples` are stored in
    * odd nodesp; the ratings are stored in edges.
    * Each node are attached a K-dimensional DenseVector[Double] storing the latent factors of
    * each node, which are randomly initialized.
    */
  private def initializeGraph(rates: RDD[Rate]) = {
    require(rates != null, "Please set training data using setTrainData method!")

    val edges = rates.map {
      case Rate(i, j, label) =>
        Edge(rowToId(i), colToId(j), label)
    }.cache()

    // compute the std value for random initialization
    val rs: RDD[Double] = edges.map(a => a.attr).cache()
    val miu: Double = math.sqrt(rs.sum() / rs.count / K)
    rs.unpersist()

    val local_K = K
    Graph.fromEdges(edges, 0L)
      .partitionBy(PartitionStrategy.EdgePartition1D)
      .mapVertices { (vid, vdata) =>
        // attach attributes to nodeIDs
        new DenseVector[Double](Array.fill(local_K)(miu))
      }.cache()
  }

  /**
    * Train the NMF model using gradient descent.
 *
    * @return  A NMFModel with W matrix and H matrix
    */
  def train(train_rates: RDD[Rate],
            valid_rates: RDD[Rate] = null
           ): NMFGraphXModel = {
    var graph: Graph[DenseVector[Double], Double] = initializeGraph(train_rates)
    var model: NMFGraphXModel = null

    val times = new ArrayBuffer[Double]()
    for (iter <- 0 to max_iter) {
      val timer = new Timer()
      // update H
      graph = updateW(graph)
      // update W
      graph = updateH(graph)
      if (model != null)
        model.clean()
      model = buildModel(graph)

      val cost_t = timer.cost()
      val msg = Msg("iter" -> iter, "time cost(ms)" -> cost_t)
      times.append(cost_t)

      val train_rmse = evaluate(train_rates, model)
      msg.append("trainRMSE", train_rmse)
      if (valid_rates != null) {
        val valid_rmse = evaluate(valid_rates, model)
        msg.append("validRMSE", valid_rmse)
      }
      logInfo(msg.toString)
    }

    logInfo("average iteration time:" + (times.sum / times.size) + "ms")
    model
  }

  /** Build a NMFGraphXModel from a graph by extracting W and H from its vertexes */
  private def buildModel(graph: Graph[DenseVector[Double], Double]): NMFGraphXModel = {
    val W = graph.vertices.filter(a => isRowId(a._1)).map {
      case (i, dv) => (idToRow(i), dv)
    }.cache()
    val H = graph.vertices.filter(a => isColId(a._1)).map {
      case (j, dv) => (idToCol(j), dv)
    }.cache()
    new NMFGraphXModel(W, H)
  }

  /** Update W by changing the attributes of source nodes in `graph` */
  private def updateW(graph: Graph[DenseVector[Double], Double]):
  Graph[DenseVector[Double], Double] = {
    updateGraph(graph, sendGradOfW)
  }

  /** Update W by changing the attributes of source nodes in `graph` */
  private def updateH(graph: Graph[DenseVector[Double], Double]):
  Graph[DenseVector[Double], Double] = {
    updateGraph(graph, sendGradOfH)
  }

  /**
    * Update the vertices using message sent by `sendMsg`
    * Consists of matrix updating and message passing two parts.
    * It's the core of this NMF alg.
    *
    * @param sendMsg  SendMsg is a message sending function.
    * @return  The active message number.
    */
  private def updateGraph(graph: Graph[DenseVector[Double], Double],
                          sendMsg: (EdgeContext[DenseVector[Double], Double, DenseVector[Double]] => Unit)):
  Graph[DenseVector[Double], Double] = {
    val prevGraph = graph
    val nodes: VertexRDD[DenseVector[Double]] = graph.aggregateMessages(sendMsg, (a, b) => a + b)
    val new_graph = graph.joinVertices(nodes)(vertexUpdate).cache()

    // materialize the new graph
    new_graph.edges.foreachPartition(_ => Unit)
    prevGraph.unpersist()
    new_graph
  }

  /**
    *
    * @param rds  (i, j, v) triples for evaluation
    * @return   RMSE
    */
  private def evaluate(rds: RDD[Rate],
                       model: NMFGraphXModel): Double = {
    val tys: RDD[(Double, Double)] = model.predict(rds).map {
      case (prediction, rate) => (rate.label, prediction)
    }
    RMSE(tys)
  }

  /**
    * Update the latent factor of each vertex using gradient descent
    *
    * @param id  Vertex id
    * @param attri  Vertex attribute
    * @param grad  A vector records the changes of the attribute
    * @return A new vector and it's the vertex's new attribute
    */
  private def vertexUpdate(id: VertexId, attri: DenseVector[Double],
                           grad: DenseVector[Double]): DenseVector[Double] = {
    val vs = attri * (1 - learn_rate * reg) + grad * learn_rate
    // set the negative value to zero
    vs.map(math.max(_, 0))
  }

  /** Map a row index to node id */
  private def rowToId(i: Int): Long = i

  /** Map a column index to node id */
  private def colToId(j: Int): Long = -(j + 1)

  /** Map a node id to row index */
  private def idToRow(i: Long): Int = i.toInt

  /** Map a node id to column index */
  private def idToCol(j: Long): Int = -(j + 1).toInt

  private def isRowId(i: Long): Boolean = i >= 0

  private def isColId(i: Long): Boolean = i < 0

  /**
    * Send messages to the source vertex with the opposite edge direction
    * grad(w_i) = h_j * (r_ij - w_i dot h_j)
    *
    * @param e type schema: [vertex attribute, edge attribute, message]
    */
  private def sendGradOfW(e: EdgeContext[DenseVector[Double], Double, DenseVector[Double]]) {
    val grad = e.dstAttr * (e.attr - e.srcAttr.dot(e.dstAttr))
    e.sendToSrc(grad)
  }

  /**
    * Send messages to the destination vertex with edge direction
    * grad(h_j) = w_i * (r_ij - w_i dot h_j)
    *
    * @param e type schema: [vertex attribute, edge attribute, message]
    */
  private def sendGradOfH(e: EdgeContext[DenseVector[Double], Double, DenseVector[Double]]) {
    val grad = e.srcAttr * (e.attr - e.srcAttr.dot(e.dstAttr))
    e.sendToDst(grad)
  }
}