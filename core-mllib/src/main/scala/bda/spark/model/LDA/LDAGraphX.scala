package bda.spark.model.LDA

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.Doc
import bda.common.stat.Counter
import bda.common.util.Timer
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * LDA GraphX Model, which stores the topic-word distribution as
  * a VertexRDD, indexed by word ids.
  * @param pwz  RDD[(wid:Int, P(w|z): DenseVector[Double])]
  */
class LDAGraphXModel(val alpha: Double,
                     val pwz: VertexRDD[DenseVector[Double]])
  extends LDAModel {

  override val W: Int = pwz.count().toInt
  override val K: Int = pwz.first._2.size

  /** number of iteration for inference */
  var max_iter: Int = 10

  if (pwz.getStorageLevel == StorageLevel.NONE) pwz.cache()


  /**
    * Evaluate the likelihood of documents. It can
    * be used for Perplexity evaluation.
    */
  def logLikelihood(docs: RDD[Doc]): Double = {
    val graph = buildGraph(docs)
    graph.triplets.map { e =>
      val (pz_d, tf, pw_z) = (e.srcAttr, e.attr, e.dstAttr)
      tf * math.log(pz_d dot pw_z)
    }.sum
  }

  /**
    * Infer P(z|d) for documents
    *
    * @return  P(z|d), a K-dimension DenseVector
    */
  def predict(docs: RDD[Doc]): RDD[DenseVector[Double]] = {
    val graph = buildGraph(docs)
    LDAGraph.getDocAttr(graph)
  }

  /**
    * Build a doc-word bipartite graph, where the attribute
    * of the document vertices is p(z|d).
    * @param docs Test documents
    */
  private def buildGraph(docs: RDD[Doc]): Graph[DenseVector[Double], Int] = {
    var graph = LDAGraph.createGraph(docs, pwz)
    for (iter <- 1 to max_iter) {
      graph = updateDocVertices(graph)
    }
    // normalize the attribute of document vertices
    val a = alpha
    graph.mapVertices {
      case (id, dv) =>
        if (LDAGraph.isDocVertex(id))
          (dv + a).normalize()
        else
          dv
    }
  }

  /** Update the document vertices with fixed word vertices */
  private def updateDocVertices(graph: Graph[DenseVector[Double], Int]):
  Graph[DenseVector[Double], Int] = {
    /** Send P(z|d, w) to w and d, and collect the counts */
    val a = alpha
    val sendTopics: EdgeContext[DenseVector[Double], Int, DenseVector[Double]] => Unit = {
      (e) => {
        val (nz_d, tf, pw_z) = (e.srcAttr, e.attr, e.dstAttr)
        // compute P(z|d,w) \propto n(z|d) * n(w|z) / n(z)
        val pz_dw: DenseVector[Double] = ((nz_d + a) * pw_z).normalize()
        val e_nz = pz_dw * tf
        e.sendToDst(e_nz)
      }
    }

    // update the topic counts of documents and words
    val new_doc_vts: VertexRDD[DenseVector[Double]] = graph.aggregateMessages(sendTopics,
      (a: DenseVector[Double], b: DenseVector[Double]) => a + b).cache()
    graph.joinVertices(new_doc_vts) {
      case (id, old_v, v) => v
    }
  }

  /** Save the model into a file */
  def save(model_pt: String): Unit = {
    logInfo("Save model into:" + model_pt)
    val alpha_pt = model_pt + "model.alpha"
    val pwz_pt = model_pt + "model.pwz"
    pwz.context.makeRDD(Seq(alpha), 1).saveAsObjectFile(alpha_pt)
    pwz.saveAsObjectFile(pwz_pt)
  }
}

object LDAGraphXModel extends Logging {

  /** Load the model into a file */
  def load(sc: SparkContext, model_pt: String): LDAGraphXModel = {
    logInfo("Load model from:" + model_pt)
    val alpha_pt = model_pt + "model.alpha"
    val pwz_pt = model_pt + "model.pwz"
    val alpha = sc.objectFile[Double](alpha_pt).first()
    val pwz = sc.objectFile[(Long, DenseVector[Double])](pwz_pt)
    new LDAGraphXModel(alpha, VertexRDD(pwz))
  }
}


private[LDA] object LDAGraph {

  /**
    * Build a bipartite graph, whose vertices include
    * - documentVertices  id < 0 (source vertices)
    * - wordVertices  id >= 0 (target vertices)
    * A edge connects a document d and a word w if w occurred in
    * d. Its term frequency in d is the weight of the edge.
    *
    * @param docs   SparseVector((w: Int, tf: Int), ...)
    * @return  A document-word bipartite graph
    */
  def createGraph(docs: RDD[Doc], K: Int): Graph[DenseVector[Double], Int] = {
    val doc_vts = createDocVertices(docs, K)
    val word_vts = createWordVertices(docs, K)
    val edges = createEdges(docs)
    Graph(doc_vts.union(word_vts), edges)
  }

  /**
    * Build a doc-word bipartite graph with existing word vertices,
    * which is used for inference.
    * @param docs   Input documents
    * @param word_vts  Existing word vertices
    * @return  A doc-word bipartite graph
    */
  def createGraph(docs: RDD[Doc], word_vts: VertexRDD[DenseVector[Double]]):
  Graph[DenseVector[Double], Int] = {
    val K = word_vts.first()._2.size
    val doc_vts = createDocVertices(docs, K)
    val edges = createEdges(docs)
    Graph(doc_vts.union(word_vts), edges)
  }

  /** Create document vertices whose vertex id is negative */
  def createDocVertices(docs: RDD[Doc],
                        K: Int): RDD[(VertexId, DenseVector[Double])] = {
    docs.zipWithIndex().map {
      case (ws, i) => (index2DocId(i), DenseVector.zeros[Double](K))
    }
  }

  /** Create word vertices. The vertex id is the word index. */
  def createWordVertices(docs: RDD[Doc],
                         K: Int): RDD[(VertexId, DenseVector[Double])] = {
    docs.flatMap(doc => doc.words).distinct().map { w =>
      (w.toLong, DenseVector.zeros[Double](K))
    }
  }

  /** create edges between each word-doc occurrence pair */
  def createEdges(docs: RDD[Doc]): RDD[Edge[Int]] =
    docs.zipWithIndex().flatMap {
      case (doc, d) =>
        Counter(doc.words).toSeq.map {
          case (w, tf) => Edge(index2DocId(d), w, tf)
        }
    }

  /** Extract doc vertices */
  def getDocVertices(graph: Graph[DenseVector[Double], Int]):
  VertexRDD[DenseVector[Double]] =
    graph.vertices.filter { case (id, _) => isDocVertex(id) }

  /** Extract word vertices */
  def getWordVertices(graph: Graph[DenseVector[Double], Int]):
  VertexRDD[DenseVector[Double]] =
    graph.vertices.filter { case (id, _) => isWordVertex(id) }

  /** Return the attribute of the docs. Keep the order of
    * input documents.
    * */
  def getDocAttr(graph: Graph[DenseVector[Double], Int]):
  RDD[DenseVector[Double]] = {
    getDocVertices(graph).map {
      case (id, dv) => (docId2Index(id), dv)
    }.sortByKey().map(_._2)
  }

  /** Transform a document index to Vertex ID */
  def index2DocId(i: Long) = -(i + 1)

  def docId2Index(i: Long) = -(i + 1)

  def isDocVertex(vertex_id: VertexId): Boolean = vertex_id < 0

  def isWordVertex(vertex_id: VertexId): Boolean = vertex_id >= 0
}

private[LDA]
class LDAGraphXTrainer(override val K: Int,
                       override val alpha: Double,
                       override val beta: Double,
                       override val max_iter: Int
                      ) extends LDATrainer[LDAGraphXModel] {

  override def train(docs: RDD[Doc]): LDAGraphXModel = {
    var graph = LDAGraph.createGraph(docs, K)

    val times = Array.fill(max_iter)(0.0)
    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      // update H
      val pre_graph = graph
      graph = EM_Update(graph)
      // materialize the graph
      graph.vertices.foreachPartition(_ => Unit)

      // unpersist the old vertices
      pre_graph.vertices.unpersist()

      val cost_t = timer.cost()
      times(iter - 1) = cost_t
      logInfo(s"iter $iter, time cost(ms)=$cost_t")
    }

    logInfo("average iteration time:" + (times.sum / times.size) + "ms")
    createModel(graph)
  }


  /** Run A iteration of EM to update the topic counts of words and documents */
  def EM_Update(graph: Graph[DenseVector[Double], Int]): Graph[DenseVector[Double], Int] = {
    // compute the number of words assigned to each topic
    val word_vts = LDAGraph.getWordVertices(graph)
    val nz: DenseVector[Double] = word_vts.values.treeReduce(_ + _) + beta * K

    /** Send P(z|d, w) to w and d, and collect the counts */
    val a = alpha
    val b = beta
    val sendTopics: EdgeContext[DenseVector[Double], Int, DenseVector[Double]] => Unit = {
      (e) => {
        val (nz_d, tf, nw_z) = (e.srcAttr, e.attr, e.dstAttr)
        // compute P(z|d,w) \propto n(z|d) * n(w|z) / n(z)
        val pz_dw: DenseVector[Double] = ((nz_d + a) * (nw_z + b) / nz).normalize()
        val e_nz = pz_dw * tf
        e.sendToDst(e_nz)
        e.sendToSrc(e_nz)
      }
    }

    // update the topic counts of documents and words
    val new_vts: VertexRDD[DenseVector[Double]] = graph.aggregateMessages(sendTopics,
      (a: DenseVector[Double], b: DenseVector[Double]) => a + b).cache()

    GraphImpl.fromExistingRDDs(new_vts, graph.edges)
  }

  /** Create a LDA GraphX model */
  private def createModel(graph: Graph[DenseVector[Double], Int]): LDAGraphXModel = {
    val word_vts = LDAGraph.getWordVertices(graph)
    val nz: DenseVector[Double] = word_vts.values.treeReduce(_ + _) + beta * K
    val pwz = word_vts.mapValues(nwz => (nwz + beta) / nz)
    new LDAGraphXModel(alpha, pwz)
  }
}