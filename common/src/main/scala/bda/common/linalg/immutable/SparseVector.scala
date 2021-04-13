package bda.common.linalg.immutable

import bda.common.linalg.{SparseVec, VecLoader}
import bda.common.stat.Counter

import scala.Numeric.Implicits._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Immutable Sparse Vector, can be used to store feature vectors
  *
  * @param _indexes  indexes of non-zero elements in increasing order
  * @param _values   values of non-zero elements at each index
  * @tparam T   value type
  */
class SparseVector[T: Numeric : ClassTag](private val _indexes: Array[Int],
                                          private val _values: Array[T])
  extends SparseVec[T] {
  assert(_indexes.size == _values.size, "index size does not equal to value size")

  /** Create a empty SparseVector */
  def this() = this(Array[Int](), Array[T]())

  /**
    * Create a SparseVector by removing the zero values in this
    * Return a new SparseVector without zero values
    */
  def compact(): SparseVector[T] = {
    val kvs = _indexes.zip(_values).filter {
      case (i, v) => v != zero
    }
    SparseVector(kvs)
  }

  /** Return the `i`th element in the Vector */
  def apply(i: Int): T = {
    val idx = _indexes.indexOf(i)
    if (idx == -1) zero else _values(idx)
  }

  /** Return the non-zero indexes and values **/
  def active: Array[(Int, T)] = _indexes.zip(_values)

  /** Return the active values **/
  def activeIndexes: Array[Int] = _indexes

  /** Return the active values **/
  def activeValues: Array[T] = _values

  /**
    * Convert into Double SparseVector.
    *
    * @note Used when performs calculation with Double type constant/Vector/Matrix.
    */
  def toDouble: SparseVector[Double] = mapActiveValues(_.toDouble)

  /** Create a SparseVector by applying `f` to each index-value pair of active element */
  def mapActive[V: Numeric : ClassTag](f: (Int, T) => V): SparseVector[V] = {
    val vs = _indexes.zip(_values).map {
      case (k, v) => f(k, v)
    }
    new SparseVector(_indexes, vs)
  }

  /** Create a SparseVector by applying `f` to each active value */
  override def mapActiveValues[V: Numeric : ClassTag](f: T => V): SparseVector[V] =
    new SparseVector(_indexes, _values.map(f))

  /** Create a SparseVector by adding this with `that` SparseVector */
  def +(that: SparseVec[T]): SparseVector[T] = {
    val ids = ArrayBuffer[Int]()
    val vs = ArrayBuffer[T]()

    var i = 0 // index of active element in this SparseVector
    that.active.foreach {
      case (j, v) =>
        // append elements in this with index less than j
        while (i < this.activeSize && _indexes(i) < j) {
          ids.append(_indexes(i))
          vs.append(_values(i))
          i += 1
        }
        if (i < this.activeSize && _indexes(i) == j) {
          // the two SparseVector share the same index
          val new_v = v + _values(i)
          // filter zero value
          if (new_v != zero) {
            ids.append(j)
            vs.append(new_v)
          }
          i += 1
        } else {
          ids.append(j)
          vs.append(v)
        }
    }
    // append the remaining elements in this SparseVector
    if (i < activeSize) {
      ids.appendAll(_indexes.takeRight(activeSize - i))
      vs.appendAll(_values.takeRight(activeSize - i))
    }
    new SparseVector(ids.toArray, vs.toArray)
  }

  /** Create a SparseVector by subtracting `that` SparseVector from this SparseVector */
  def -(that: SparseVec[T]): SparseVector[T] = {
    val ids = ArrayBuffer[Int]()
    val vs = ArrayBuffer[T]()
    var i = 0
    that.active.foreach {
      case (j, v) =>
        // append elements in this with index less than j
        while (i < activeSize && _indexes(i) < j) {
          ids.append(_indexes(i))
          vs.append(_values(i))
          i += 1
        }
        if (i < activeSize && _indexes(i) == j) {
          // the two SparseVector share the same index
          val new_v = _values(i) - v
          if (new_v != zero) {
            ids.append(j)
            vs.append(new_v)
          }
          i += 1
        } else {
          ids.append(j)
          vs.append(-v)
        }
    }

    // append the remaining elements in this SparseVector
    if (i < activeSize) {
      ids.appendAll(_indexes.takeRight(activeSize - i))
      vs.appendAll(_values.takeRight(activeSize - i))
    }
    new SparseVector(ids.toArray, vs.toArray)
  }

  /**
    * Element-wise product with anther SparseVector
    * Return a SparseVector
    */
  def *(that: SparseVec[T]): SparseVector[T] = {
    val ids = ArrayBuffer[Int]()
    val vs = ArrayBuffer[T]()
    var i = 0
    that.active.foreach {
      case (j, v) =>
        while (i < activeSize && _indexes(i) < j)
          i += 1

        if (i < activeSize && _indexes(i) == j) {
          ids.append(j)
          vs.append(_values(i) * v)
          i += 1
        }
    }
    new SparseVector(ids.toArray, vs.toArray)
  }
}

object SparseVector extends VecLoader[SparseVector] {

  /** Create a SparseVector from a index and value sequence */
  def apply[T: Numeric : ClassTag](indexes: Array[Int], values: Array[T]) =
    new SparseVector[T](indexes, values)

  /** Create a SparseVector from a index and value sequence */
  def apply[T: Numeric : ClassTag](indexes: Seq[Int], values: Seq[T]) =
    new SparseVector[T](indexes.toArray, values.toArray)

  /** Create a SparseVector from a key-value sequence */
  def apply[T: Numeric : ClassTag](kvs: Seq[(Int, T)]): SparseVector[T] = {
    val (ks, vs) = kvs.unzip
    SparseVector(ks, vs)
  }

  /** Create a SparseVector from a Map */
  def apply[T: Numeric : ClassTag](kvs: Map[Int, T]): SparseVector[T] =
    SparseVector(kvs.toSeq)

  /** Create a SparseVector from a key-value array */
  def apply[T: Numeric : ClassTag](kvs: Array[(Int, T)]): SparseVector[T] = {
    val (ks, vs) = kvs.unzip
    SparseVector(ks, vs)
  }

  /**
    * Create a SparseVector by count the frequency of indexes in a Sequence
    *
    * Example: SparseVector(3, List(1,1,2,)) = SparseVector(3, Seq(1->2, 2->1))
    *
    * @param items  index sequence
    */
  def apply(items: TraversableOnce[Int]): SparseVector[Int] = {
    val kvs = Counter(items).toSeq
    SparseVector[Int](kvs)
  }

  /** Create a empty SparseVector with fixed size */
  def empty[T: Numeric : ClassTag](size: Int = 0): SparseVector[T] = {
    new SparseVector(Array.empty[Int], Array.empty[T])
  }

  /**
    * Parse a Double SparseVector
    *
    * @todo add try-catch to tackle bad format string
    * @param s  format:"size i:v,i:v,..."
    */
  def parse(s: String): SparseVector[Double] =
    if (s.isEmpty) {
      new SparseVector(Array[Int](), Array[Double]())
    }
    else {
      val (indexes, values) = s.split("[ |,]").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toDouble)
      }.unzip
      new SparseVector(indexes.toArray, values.toArray)
    }

  /**
    * Parse a Float SparseVector
    *
    * @todo add try-catch to tackle bad format string
    * @param s  format:"size i:v,i:v,..."
    */
  def parseFloat(s: String): SparseVector[Float] =
    if (s.isEmpty) {
      new SparseVector(Array[Int](), Array[Float]())
    }
    else {
      val (indexes, values) = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toFloat)
      }.unzip
      new SparseVector(indexes.toArray, values.toArray)
    }

  /**
    * Parse a Double SparseVector
    *
    * @todo add try-catch to tackle bad format string
    * @param s  format:"size i:v,i:v,..."
    */
  def parseInt(s: String): SparseVector[Int] =
    if (s.isEmpty) {
      new SparseVector(Array[Int](), Array[Int]())
    }
    else {
      val (indexes, values) = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toInt)
      }.unzip
      new SparseVector(indexes.toArray, values.toArray)
    }

  /**
    * Parse a Long SparseVector
    *
    * @todo add try-catch to tackle bad format string
    * @param s format:"size i:v,i:v,..."
    */
  def parseLong(s: String): SparseVector[Long] =
    if (s.isEmpty) {
      new SparseVector(Array[Int](), Array[Long]())
    }
    else {
      val (indexes, values) = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toLong)
      }.unzip
      new SparseVector(indexes.toArray, values.toArray)
    }
}
