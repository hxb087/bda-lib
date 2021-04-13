package bda.common.util

import scala.collection.mutable.{Map => MuMap}
import scala.reflect.ClassTag


/**
  * Utility functions for Map
  */
object MapUtil {

  /**
    * Convert a map to a Vector
    * @deprecated  Using SparseVector.toDense instead
    * @param size  The size of result vector. If size=-1, it will choose
    *              the maximum value of `map` to be the size.
    */
  def toVector[V: Numeric](map: Map[Int, V], size: Int = -1): Vector[V] = {
    val maxIndex: Int = if (size == -1) map.keys.max else size - 1
    (0 to maxIndex).map { i =>
      map.getOrElse(i, implicitly[Numeric[V]].zero)
    }.toVector
  }

  /** Create a map by swapping the key and value in `map` */
  def swap[K, V](map: Map[K, V]): Map[V, K] = map.map {
    case (k, v) => (v, k)
  }

  /**
    * Generate a String representation of a Map
    * @param map   Map to format a string
    * @param delim  delimiter of key-values, default is " "
    * @return  a String with format "k:v k:v ..."
    */
  def str[K, V](map: Map[K, V], delim: String = ","): String = map.map {
    case (k, v) => s"$k:$v"
  }.mkString(delim)

  /**
    * convert `s` to a string Map
    * @param  s: "k:v ...."
    * @return {k:v, ...}
    */
  def parse(s: String,
            delim: String = ","): Map[String, String] =
    s.split(delim).map { kv =>
//     val Array(k, v) = kv.split(":").filterNot(_.isEmpty)
//      println(s"k=${k},v=${v}" )
//      (k, v)
      val index = kv.lastIndexOf(":")
//      println(s"kv=${kv},index=${index},length=${kv.length}")
      (kv.substring(0,index),kv.substring(index+1,kv.length))

    }.toMap

  /**
    * Sum the values of the same keys in two map
    */
  def sum[K: ClassTag, V: ClassTag : Numeric](map1: Map[K, V],
                                              map2: Map[K, V]): Map[K, V] =
    aggregate(map1, map2)(implicitly[Numeric[V]].plus)

  /**
    * Create a new Map by aggregating two maps
    *
    * @return A new Map contains all the keys of map1 and map2, which
    *         the common elements will be aggregating by function `f`.
    */
  def aggregate[K: ClassTag, V: ClassTag](map1: Map[K, V],
                                          map2: Map[K, V])
                                         (f: (V, V) => V): Map[K, V] = {
    val mumap = MuMap[K, V](map1.toSeq: _*)
    for ((k, v) <- map2) {
      if (map1.contains(k))
        mumap(k) = f(map1(k), map2(k))
      else
        mumap(k) = map2(k)
    }
    mumap.toMap
  }
}

/**
  * Utility functions for Map of a Map
  */
object MapMapUtil {
  /**
    * Transform the inter-key and the outer-key of a MapMap
    * @return {k2:{k1:v, ...}, ...}
    */
  def inverse[K1, K2, V](mmap: Map[K1, Map[K2, V]]): Map[K2, Map[K1, V]] = {
    val t = MuMap[K2, MuMap[K1, V]]()
    mmap.foreach {
      case (k1, kvs) => kvs.foreach {
        case (k2, v) => t(k2)(k1) = v
      }
    }
    t.map {
      case (k1, kvs) => (k1, kvs.toMap)
    }.toMap
  }

  /**
    * Convert a map of map to (k1, k2, v) triples
    * @return Seq[(key1, key2, value)]
    */
  def toTriples[K1, K2, V](mmap: Map[K1, Map[K2, V]]): Seq[(K1, K2, V)] =
    mmap.flatMap {
      case (k1, kvs) => kvs.map {
        case (k2, v) => (k1, k2, v)
      }
    }.toSeq
}