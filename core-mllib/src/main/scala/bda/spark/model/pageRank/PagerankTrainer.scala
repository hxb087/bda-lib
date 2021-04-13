package bda.spark.model.pageRank

import bda.common.Logging
import bda.common.util.{Msg, Timer}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer


/** pagerank Model class. */
class PagerankModel(val values: RDD[(VertexId, Double)]) extends Logging {

  /**
   * PageRank value join with name.
   * @param name An RDD in format as [id, name]
   * @return Name and pagerank tuples
   */
  def joinNodeName(name: RDD[(Long, String)]): RDD[(String, Double)] = {
    val PR_with_name: RDD[(String, Double)] = values.leftOuterJoin(name).map {
      case (id, (pr, name))
      => (name.getOrElse("null"), pr)
    }
    PR_with_name
  }


  /** Get topK user's PR value. */
  def getTopK(topK: Int): Array[(Long, Double)] = {
    values.sortBy(f => f._2, false).take(topK)
  }

  /** save pagerank value to HDFS */
  def saveModel(pt: String): Unit = {
    logInfo(s"save model: $pt")
    values.saveAsObjectFile(pt)
  }

  /** save as hdfs file. */
  def saveAsTextFile(pt: String): Unit = {
    logInfo(s"save model: $pt")
    values.saveAsTextFile(pt)
  }
}

/** Factory of pagerankModel */
object PagerankModel extends Logging {

  def load(sc: SparkContext, model_pt: String): PagerankModel = {
    logInfo("save model into: " + model_pt)
    val PR = sc.objectFile[(Long, Double)](model_pt)
    val model = new PagerankModel(PR)
    model
  }
}


/** PageRank training class. */
private[pageRank]
class PagerankTrainer(damping_factor: Double = 0.85,
                      max_iter: Int = 100) extends Serializable with Logging{
  //num of vertices whose outdegree is 0
  private var K: Long = 0
  //num of vertices in pagerank graph
  private var N: Long = 0

  /**
   * Update the vertex with new pagerank.
   * @param id vertex_id
   * @param attr old_pr
   * @param msg_sum Aggregated pr/out_degree
   * @return
   */
  def updateVertex(id: VertexId, attr: Double, msg_sum: Double): Double = {
    damping_factor * msg_sum
  }

  def sum(a: Double, b: Double): Double = a + b

  /**
   * Send pr/out_degree with edge direction.
   * @param e
   */
  def sendPR(e: EdgeContext[Double, Double, Double]) = {
    e.sendToDst(e.srcAttr * e.attr)
  }

  /**
   * Run pagerank algorithm with damping_factor and max_iter.
   * @param g The original graph.
   * @return An RDD includes all vertices' id and pagerank value.
   */
  def run(g: Graph[Double, Double]): PagerankModel = {
    N = g.vertices.count()
    //Cache the outdegree to get the vertices whose outdegree is 0.
    val outdegree = g.outDegrees.cache()

    //Initialize the graph with its pr set 1/N and edge attr set 1/out_degree
    var graph = g.outerJoinVertices(outdegree) {
      case (vid, vdata, deg) => deg.getOrElse(0)
    }.mapTriplets(e => 1.0 / e.srcAttr)
      .mapVertices((id, attr) => 1.0 / N)
      .cache()

    // v is a vertices set whose out_degree  > 0.
    val v: VertexRDD[Double] = graph.vertices.innerJoin(outdegree) { (vid, PR, deg) => PR }
    // special_vertices are vertices whose out_degree is 0.
    val special_vertices = graph.vertices.subtract(v)
    special_vertices.checkpoint()

    //send messages:[ pr / out_degree ].
    //aggregate messages to the same dstId.
    var messages: VertexRDD[Double] = graph.aggregateMessages(sendPR, sum).cache()
    var old_message: VertexRDD[Double] = null

    //K is the num of vertices whose out_degree is 0.
    K = special_vertices.count()
    outdegree.unpersist(blocking = false)

    //avg_PR_deg1 is average PageRank with out_degree >= 1.
    //avg_PR_deg0 is average PageRank with out_degree == 0.
    var avg_PR_deg1 = (1.0 - K * 1.0 / N) / N
    var avg_PR_deg0 = 1.0 * K / (N * N)

    var preG: Graph[Double, Double] = null
    val times = new ArrayBuffer[Double]()
    var iter = 0
    while (iter < max_iter) {
      val timer = new Timer()
      val new_PR: VertexRDD[Double] = graph.vertices
        .innerJoin(messages)(updateVertex).cache()

      preG = graph
      //update tht pagerank value.
      graph = graph.outerJoinVertices(new_PR) {
        case (vid, old_PR, new_opt) => new_opt.getOrElse(0.0)
      }.mapVertices {
        (id, PR) => PR + avg_PR_deg1 * (1 - damping_factor) + avg_PR_deg0
      }.cache()

      if(iter < max_iter - 1 && iter % 3 == 0){
        graph.vertices.checkpoint()
      }

      old_message = messages
      messages = graph.aggregateMessages(sendPR, sum).cache()

      //pr sum of those vertices whose outdegree is 0.
      val sum0_PR = graph.vertices.innerJoin(special_vertices) {
        case (id, pr, zero) => pr
      }.map {
        case (id, attr) => attr
      }.sum()

      avg_PR_deg1 = (1.0 - sum0_PR) / N
      avg_PR_deg0 = sum0_PR / N


      val cost_time = timer.cost()
      times.append(cost_time)
      val msg = Msg(s"iter" -> iter, s"cost_time(ms)" -> cost_time)
      logInfo(msg.toString)

      old_message.unpersist(blocking = false)
      if(iter < max_iter - 1 && iter % 3 != 1) {
        preG.unpersistVertices(blocking = false)
        preG.edges.unpersist(blocking = false)
      }
      new_PR.unpersist(blocking = false)
      iter = iter + 1
    }
    val avg_time = times.sum / iter
    logInfo(s"average iteration time is: $avg_time ms.\n")
    val PR: RDD[(VertexId, Double)] = graph.vertices

    val prmodel = new PagerankModel(PR)
    prmodel
  }
}


/** User interface for pagerank. */
object PageRank extends Logging{

  /**
   * Spark pagerank fitting
   * @param g Procesiong graph.
   * @param max_iter  The maximum number of iterations.
   * @param damping_factor The prob of page diverting along links.
   * @return vertex id and  pagerank value pair.
   */
  def run(g: Graph[Double, Double],
          max_iter: Int = 15,
          damping_factor: Double = 0.85): PagerankModel = {
    val msg = Msg("n(g.vertices): " -> g.vertices.count(),
      "n(g.edges): " -> g.edges.count,
      "max_iteration: " -> max_iter,
      "dampinf_factor: " -> damping_factor)
    logInfo(msg.toString)

    val PR  = new PagerankTrainer(damping_factor, max_iter)
      .run(g)
    PR
  }
}