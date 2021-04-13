package bda.spark.model.KShell

import bda.common.Logging
import bda.common.util.{Msg, Timer}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** KShell Algorithm
  * KShell alg is an influence qualification algorithm,
  * every node has a value K, it means the degree when it‘s deleted.
  * The biggest K the vertices has, the most influential it is.
  *
  */

/** KShell Model class. */
class KShellModel(val shell: RDD[(VertexId, Int)]) extends Logging {

  /**
   * Kshell value join with name.
 *
   * @param name An RDD in format as [id, name]
   * @return Name and Kshell tuples
   */
  def joinNodeName(name: RDD[(Long, String)]): RDD[(String, Int)] = {
    val k_shell_with_name: RDD[(String, Int)] = shell.leftOuterJoin(name).map {
      case (id, (pr, name))
      => (name.getOrElse("null"), pr)
    }
    k_shell_with_name
  }

  /** Get topK user's k_shell value. */
  def getTopK(topK: Int): Array[(Long, Int)] = {
    shell.sortBy(f => f._2, ascending = false).take(topK)
  }

  /** save KShell model to HDFS */
  def saveModel(model_pt: String): Unit = {
    logInfo(s"save model into $model_pt")
    shell.saveAsObjectFile(model_pt)
  }

  /** save as HDFS file. */
  def saveAsTextFile(model_pt: String): Unit = {
    logInfo(s"save model into: $model_pt")
    shell.saveAsTextFile(model_pt)
  }
}

/** Factory of KShellModel */
object KShellModel extends Logging {
  def load(sc: SparkContext, model_pt: String): KShellModel = {
    logInfo(s"load model from: $model_pt")
    val k_shell = sc.objectFile[(Long, Int)](model_pt)
    val model = new KShellModel(k_shell)
    model
  }
}

class KShellTrainer[VD: ClassTag, ED: ClassTag](dir: String,
                                                weight: Boolean) extends Serializable with Logging {

  private def updateGraph(g: Graph[Double, Double], K: Int): Graph[Double, Double] = {
    // Update the vertices attr as the deg
    // indegree
    var graph: Graph[Double, Double] = null
    if (dir == "indegree") {
      val ver_attr = g.aggregateMessages[Double](tri => sendDegDst(tri, K), _ + _)
      graph = g.subgraph(vpred = (id, deg) => deg > K)
      graph = graph.joinVertices(ver_attr) {
        case (id, attr, weight_deg) =>
          attr - weight_deg
      }
    }
    else {
      val ver_attr = g.aggregateMessages[Double](tri => {
        sendDegSrc(tri, K)
        sendDegDst(tri, K)
      },
        _ + _)
      graph = g.subgraph(vpred = (id, deg) => deg > K)
      graph = graph.joinVertices(ver_attr) {
        case (id, attr, weight_deg) =>
          attr - weight_deg
      }
    }
    graph
  }

  /** Send deg to its neighbours */
  private def sendDegDst(tri: EdgeContext[Double, Double, Double], K: Int) {
    if (tri.srcAttr <= K && tri.dstAttr > K) {
      tri.sendToDst(tri.attr)
    }
  }

  /** Send deg to its neighbours */
  private def sendDegSrc(tri: EdgeContext[Double, Double, Double], K: Int) {
    if (tri.dstAttr <= K && tri.srcAttr > K)
      tri.sendToSrc(tri.attr)
  }

  private def sum(a: Double, b: Double): Double = a + b

  /** Train Kshell Model. */
  def run(g: Graph[Double, Double]): KShellModel = {
    //initialization.

    var graph: Graph[Double, Double] = null
    var old_graph: Graph[Double, Double] = null

    //Init edge attr as weight or 1.0.
    graph = g
    if (!weight) {
      graph = graph.mapEdges(e => 1.0)
    }

    //Init vertex attr as degree or weighted degree.
    var deg: VertexRDD[Double] = null
    if (dir == "indegree")
      deg = graph.aggregateMessages[Double](tri => tri.sendToDst(tri.attr), _ + _).cache()
    else
      deg = graph.aggregateMessages[Double](tri => {
        tri.sendToDst(tri.attr);
        tri.sendToSrc(tri.attr)
      }, _ + _).cache()

    old_graph = graph
    graph = graph.joinVertices(deg) {
      case (id, attr, d) =>
        d
    }.cache()

    var K = 0
    val hist_time = new ArrayBuffer[Double]()

    var count = graph.vertices.count
    deg.unpersist()

    var result: RDD[(VertexId, Int)] = graph.vertices.filter(a => a._2 <= -1)
      .map(a => (a._1, -1))
    //test whether the nodes are all deleted.
    while (count > 0) {
      val timer = new Timer()
      var K_result = graph.vertices.filter(a => a._2 <= K).map(a => (a._1, K)).cache()
      var ver = graph.vertices.filter(a => a._2 <= K).count()
      var iter = 0
      while (ver > 0) {
        old_graph = graph
        graph = updateGraph(graph, K).cache()
        // to avoid stackOverFlow in one K.
        if (iter > 20) {
          graph.checkpoint()
          iter = 0
        }
        val temp = graph.vertices.filter(a => a._2 <= K)
          .map(a => (a._1, K)).cache()
        ver = temp.count()

        K_result = K_result.union(temp).cache()
        old_graph.unpersist()
        //temp.unpersist()
        iter += 1
      }
      val old_count = count
      count = graph.vertices.count
      val time_cost = timer.cost()
      hist_time.append(time_cost)
      val msg = Msg("K" -> K, "Time(ms)" -> time_cost, "Deleted Nodes" -> (old_count - count))
      logInfo(msg.toString)

      if (count > 0) {
        println(s"There are ${count}" + " vertices are left in the graph!")
        if (K % 3 == 1) {
          if(K % 10 == 0) {
            graph = Graph(graph.vertices, graph.edges, 0.0, StorageLevel.MEMORY_ONLY, StorageLevel.MEMORY_ONLY).cache()
          }
          graph.checkpoint()
          result.checkpoint()
        }
      }
      val old_result = result
      result = old_result.union(K_result).cache()
      result.foreachPartition(_ => Unit)
      old_result.unpersist()

      K += 1
    }
    if (K > 1) {
      val m = Msg("average time(ms)" -> hist_time.sum / (K - 1), "maximum cores is" -> (K - 1))
      logInfo(m.toString)
    }
    println(result.count)
    new KShellModel(result)
  }
}