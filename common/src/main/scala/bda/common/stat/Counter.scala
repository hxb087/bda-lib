package bda.common.stat

import bda.common.util.io
import bda.common.util.io.{readMap, writeLines}

import scala.collection.mutable.{Map => MuMap}

/**
  * A counter that records the frequency of some object.
 *
 * @tparam T  object type
 * @tparam V  count type, e.g., Int, Long, Float, Double
 */
class Counter[T, V: Numeric](private val cnt: MuMap[T, V]) extends Serializable {

  /** Create a empty Counter */
  def this() = this(MuMap[T, V]())

  /** Return the count of a element */
  def apply(e: T): V = cnt.getOrElse(e, implicitly[Numeric[V]].zero)

  /** Return the number of existing objects */
  def size = cnt.size

  /** Return the most frequent object and its count */
  def max: (T, V) = cnt.maxBy(_._2)

  /** Return the most infrequent object and its count */
  def min: (T, V) = cnt.minBy(_._2)

  /** Batch add */
  def add(es: Iterable[T]): this.type = {
    es.foreach { e => this.add(e) }
    this
  }

  /** Increase the count of `e` by `n` */
  def add(e: T, n: V = implicitly[Numeric[V]].one): this.type = {
    val zero = implicitly[Numeric[V]].zero
    cnt(e) = implicitly[Numeric[V]].plus(cnt.getOrElse(e, zero), n)
    this
  }

  /** Create a new Counter by aggregating two Counters */
  def ++(that: Counter[T, V]): Counter[T, V] = {
    val kvs = (this.keys.toSet ++ that.keys.toSet).map {
      case k => (k, implicitly[Numeric[V]].plus(this(k), that(k)))
    }.toMap
    new Counter(kvs)
  }

  /** Create a Counter from map */
  def this(kvs: Map[T, V]) = this(MuMap(kvs.toSeq: _*))

  /** Return the existing objects */
  def keys: Iterable[T] = cnt.keys

  /** Aggregate the counts in `that` into this Counter */
  def ++=(that: Counter[T, V]): this.type = {
    val zero = implicitly[Numeric[V]].zero
    that.cnt.foreach {
      case (k, v) =>
        cnt(k) = implicitly[Numeric[V]].plus(cnt.getOrElse(k, zero), v)
    }
    this
  }

  /** Convert this counter into a map */
  def toMap: Map[T, V] = cnt.toMap

  /** Convert this counter into a sequence */
  def toSeq: Seq[(T, V)] = cnt.toSeq

  /** Convert this counter into a Iterable object */
  def toIterable: Iterable[(T, V)] = cnt


  /** Convert this counter into a Array */
  def toArray: Array[(T, V)] = cnt.toArray

  /**
   * Return the `n` most frequent elements
    *
    * @return Seq[(object, count)]
   */
  def top(n: Int): Seq[(T, V)] =
    cnt.toSeq.sortBy(_._2).takeRight(n).reverse

  /** Return the key-count pairs sorted by counts in decreasing order */
  def sorted: Seq[(T, V)] =
    cnt.toSeq.sortBy(_._2).reverse

  /** Return the `n` most frequent objects */
  def topKeys(n: Int): Seq[T] =
    cnt.toSeq.sortBy(_._2).takeRight(n).map(_._1).reverse

  /**
   * Create a string representation of this object
    *
    * @return a multiple string. Each line is with format: "object:count".
   */
  override def toString: String = cnt.map {
    case (e, n) => s"$e:$n"
  }.mkString("\n")

  /**
   * write to file in format: term   freq
   */
  def save(res_pt: String, threshold: V = implicitly[Numeric[V]].zero): Unit = {
    val sorted_bfs = cnt.toSeq.filter {
      case (k, v) => implicitly[Numeric[V]].gt(v, threshold)
    }.sortBy(_._2).reverse.map {
      case (k, v) => s"$k\t$v"
    }
    writeLines(res_pt, sorted_bfs)
  }
}


/**
 * Factory of Counter
 */
object Counter {

  /** Create a counter from a TraversableOnce object */
  def apply[T](es: TraversableOnce[T]): Counter[T, Int] = {
    val kvs = MuMap[T, Int]()
    es.foreach {
      case e => kvs(e) = kvs.getOrElse(e, 0) + 1
    }
    new Counter(kvs.toMap)
  }

  /**
   * Create a counter from a TraversableOnce key-value pairs
   *
   * @note The values of repeated keys will be summarized
   * @param kvs key-value pairs
   * @tparam T  key type
   * @tparam V  value type
   */
  def fromKVs[T, V: Numeric](kvs: TraversableOnce[(T, V)]): Counter[T, V] = {
    val cnt = new Counter[T, V]()
    kvs.foreach {
      case (k, v) => cnt.add(k, v)
    }
    cnt
  }

  /**
   * Load a Int Counter from file `pt`
 *
   * @param pt each line with format: "k   counter"
   */
  def loadInt(pt: String): Counter[String, Int] = {
    val cnt = readMap(pt).mapValues(_.toInt)
    new Counter(cnt)
  }

  /**
   * Load a Int Counter from file `pt`
 *
   * @param pt each line with format: "k   counter"
   */
  def loadDouble(pt: String): Counter[String, Double] = {
    val cnt = readMap(pt).mapValues(_.toDouble)
    new Counter(cnt)
  }
}