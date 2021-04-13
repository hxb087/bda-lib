package bda.spark.reader

import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.reflect.{ClassTag}


/**
 * Load a graph From an edge file.
 * Each line is an edge tuple or triple, like
 * [src_id  dst_id   (value)]
 *
 * where srcid and dstid are required, and value is optional.
 *
 * @ User can denote the direction of the file edge direction.
 *
 * For example, twitter edge format is like [dst_id src_id], User can denote direction to make
 * sure the edge are in right direction in Edge.
 *
 */
object GraphLoader {


  /**
   * Parse (i, j, (v)) tuples as edges, and Return a graph.
   *
   * @param edge edge file path.
   * @param dir default is true, if it's set false, the edge is overturn.
   * @return  A graph.
   */
  def loadGraph[A: ClassTag, B: ClassTag](edge: RDD[(Long, Long, B)],
                      default_vertices_attr: A,
                      dir: Boolean = true): Graph[A, B] = {

    val edges = edge.map {
      case (i, j, v) =>
        if (dir) {
          Edge(i, j, v)
        }
        else {
          Edge(j, i, v)
        }
      case other => throw new IllegalArgumentException(s"Bad format input: $other")
    }
    val graph = Graph.fromEdges(edges, default_vertices_attr, StorageLevel.MEMORY_ONLY,
      StorageLevel.MEMORY_ONLY).partitionBy(PartitionStrategy.EdgePartition2D)
    graph
  }

  /**
   * Parse (i, j, (v)) tuples as edges, and Return a graph.
   *
   * @param edge edge file path.
   * @param dir default is true�� if it's set false, the edge is overturn.
   * @return  A graph.
   */
  //TODO...重构方法   lijg
  def loadGraph_AB[A: ClassTag, B: ClassTag](edge: RDD[(Long, Long)],
                      default_vertices_attr: A,
                      default_edge_attr: B,
                      dir: Boolean = true
                       ): Graph[A, B] = {
    val edges = edge.map {
      case (i, j) =>
        if (dir) {
          Edge(i, j, default_edge_attr)
        }
        else {
          Edge(j, i, default_edge_attr)
    }
      case other => throw new IllegalArgumentException(s"Bad format input: $other")
    }
    val graph = Graph.fromEdges(edges, default_vertices_attr, StorageLevel.MEMORY_ONLY,
      StorageLevel.MEMORY_ONLY).partitionBy(PartitionStrategy.EdgePartition2D)
    graph
  }
}
