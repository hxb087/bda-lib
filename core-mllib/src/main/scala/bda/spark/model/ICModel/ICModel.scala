package bda.spark.model.ICModel

import bda.common.Logging
import bda.common.util.{Msg, Timer}
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * This Algorithm aim to quantify theKShell$
KShellTrainer.scala influence of some seeds in a given social network.
 * 1. In each iteration, the alg will do a random process. That is, seed first to activate its neighbours with a random
 * Strength[0, 1], And all the vertices in a graph has a same threshold to determine whether it's activated. Only the
 * newly activated vertices can activate neighbours. Do this precess until there is no active vertices anymore. The
 * last activeted vertices num is influence quantification.
 *
 * 2. Because this is a random process, so only done this process a certain times can the active vertices num become
 * stable.
 *
 * 3. Finale influence qualification is the average active vertices num.
 */
private[ICModel]
class ICModel[VD: ClassTag, ED: ClassTag](seeds: Set[Long],
                                          max_iter: Int,
                                          threshold: Double) extends Logging with Serializable {
  //private val rand = scala.util.Random

  /**
   * Initialize the graph with the seeds' state set True.
 *
   * @param g The graph used to run ICModel.
   * @return Initialized graph.
   */
  private def initialize[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]): Graph[Boolean, ED] = {
    val sc = g.vertices.sparkContext
    val seed = sc.parallelize(seeds.map(a => (a, true)).toSeq)
    val graph: Graph[Boolean, ED] = g.outerJoinVertices(seed) {
      case (vid, vdata, seed) => seed.getOrElse(false)
    }
    graph
  }

  /** Reduce function. This case, if any of the element is true, it returns true. */
  private def sum(a: Boolean, b: Boolean): Boolean = a || b

  /**
    * Activate the vertices.
 *
   * @param e edge triplets.
   */
  private def activateVertice(e: EdgeContext[Boolean, ED, Boolean]) {
    if (e.srcAttr && !e.dstAttr && scala.util.Random.nextDouble() >= threshold)
      e.sendToDst(true)
  }

  /**
   * Run ICModel.
 *
   * @param g The graph used to run ICModel.
   * @return
   */
  def run(g: Graph[VD, ED]): Double = {
    var iter: Int = 0
    var total_active: Long = 0
    val hist_time = ArrayBuffer[Double]()
    while (iter < max_iter) {
      val timer = new Timer()

      var graph: Graph[Boolean, ED] = initialize(g).cache()
      var preG: Graph[Boolean, ED] = null

      var state: VertexRDD[Boolean] = graph.aggregateMessages(activateVertice, sum).cache()
      var each_active_count: Long = state.count()
      var active_count = each_active_count

      var i = 0
      while (each_active_count > 0) {
        preG = graph
        //filter vertices who has activated neighbour.
        graph = graph.subgraph(e => e.srcAttr != true).cache()
        graph = graph.outerJoinVertices(state) {
          case (id, old_state, new_state) => new_state.getOrElse(false)
        }.cache()

        val old_state = state
        //new activated node starts to activate inactive nodes.
        state = graph.aggregateMessages(activateVertice, sum)

        each_active_count = state.count()
        active_count += each_active_count

        if(i > 0){
          preG.unpersistVertices(blocking = false)
          preG.edges.unpersist(blocking = false)
        }
        i += 1
        old_state.unpersist(blocking = false)
      }
      total_active += active_count
      iter = iter + 1

      val time_cost = timer.cost()
      hist_time.append(time_cost)
      val msg = Msg(s"iter" -> iter, s"iteration time(ms)" -> time_cost, s"accum activated vertices" -> total_active)
      logInfo(msg.toString)
    }
    val m = Msg(s"average iteration time"-> hist_time.sum / max_iter)
    logInfo(m.toString)
    total_active * 1.0 / iter
  }
}

/** ICModel class factory. */
object ICModel extends Logging {

  /** User Interface. */
  def run(g: Graph[Int, Int], seed: Set[Long], max_iter: Int, threhold: Double): Double = {
    val msg = Msg("n(g.vertices)" -> g.vertices.count(),
      "n(g.edges)" -> g.edges.count,
      "max_iteration" -> max_iter,
      "threshold" -> threhold)
    logInfo(msg.toString)

    val icmodel = new ICModel[Int, Int](seed, max_iter, threhold)
    icmodel.run(g)
  }
}