package bda.common.linalg.mutable

import bda.common.linalg.immutable.{SparseVector => ISV}
import bda.common.linalg.{SparseVec, VecLoader}
import bda.common.stat.Counter

import scala.Numeric.Implicits._
import scala.collection.mutable.{Map => MuMap}
import scala.reflect.ClassTag

/**
  * Mutable SparseVector stores the active indexes and values in
  * a mutable map
  *
  * @param _kvs   active indexes and values
  */
class SparseVector[T: Numeric : ClassTag](private val _kvs: MuMap[Int, T])
  extends SparseVec[T] {

  def this() = this(MuMap.empty[Int, T])

  // remove zero when create
  selfCompact()

  /** Remove zero-value elements */
  private[linalg] def selfCompact(): this.type = {
    val ks = _kvs.filter {
      case (k, v) => v == zero
    }.keys

    ks.foreach { k =>
      _kvs.remove(k)
    }
    this
  }

  /** Return the ith element in this SparseVector */
  def apply(i: Int) = {
    _kvs.getOrElse(i, zero)
  }

  /** Set the value of `i`th element to v */
  def update(i: Int, v: T): Unit = {
    if (v == zero)
      _kvs.remove(i)
    else
      _kvs(i) = v
  }

  /** Copy the elements in `that` SparseVector to this */
  def copy(that: SparseVector[T]): Unit = {
    _kvs.clear()
    _kvs ++= that._kvs
  }

  /** Generate a copy of this SparseVector */
  override def clone = new SparseVector(_kvs.clone())

  /** Return the active index-value pairs in increasing order by index */
  def active: Array[(Int, T)] = _kvs.toArray.sortBy(_._1)

  /** Return the active indexes */
  def activeIndexes: Array[Int] = active.map(_._1)

  /** Return the active values */
  def activeValues: Array[T] = active.map(_._2)

  /** Convert to a Double SparseVector */
  def toDouble: SparseVector[Double] = mapActiveValues(_.toDouble)

  /** Convert into a immutable SparseVector */
  def toImmutable: ISV[T] = {
    val (indexes, values) = this.active.unzip
    ISV(indexes.toArray, values.toArray)
  }

  /** Remove all active elements */
  def clear(): this.type = {
    _kvs.clear()
    this
  }

  /** Create a SparseVector by applying function `f` on the active index-value pairs */
  def mapActive[V: Numeric : ClassTag](f: (Int, T) => V): SparseVector[V] = {
    val map = _kvs.map {
      case (k, v) => (k, f(k, v))
    }
    new SparseVector(map)
  }

  /** Create a SparseVector by applying function `f` on the active values */
  def mapActiveValues[V: Numeric : ClassTag](f: T => V): SparseVector[V] = {
    val map = _kvs.mapValues(v => f(v))
    new SparseVector(MuMap(map.toSeq: _*))
  }


  /** Multiply constant `x` to each value in this SparseVector */
  def *=(x: T): this.type = {
    if (x == zero) {
      this.clear()
    } else {
      _kvs.foreach {
        case (k, v) => _kvs(k) = v * x
      }
      this
    }
  }

  /**
    * Divide constant `x` from each value.
    *
    * @note available whe T is Float or Double. SparseVector with Int or Long
    *       should use toDouble to convert into Double Vector first.
    */
  def /=(x: T): this.type = {
    require(x != zero)
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        _kvs.foreach {
          case (k, v) => _kvs(k) = num.div(v, x)
        }
        this
      case _ =>
        throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Create a SparseVector by adding another SparseVector */
  def +(that: SparseVec[T]): SparseVector[T] = {
    val map = this._kvs.clone()
    that.active.foreach {
      case (i, v) => map(i) = map.getOrElse(i, zero) + v
    }
    SparseVector(map)
  }

  /** Create a SparseVector by subtracting another SparseVector */
  def -(that: SparseVec[T]): SparseVector[T] = {
    val map = this._kvs.clone()
    that.active.foreach {
      case (i, v) => map(i) = map.getOrElse(i, zero) - v
    }
    SparseVector(map)
  }

  /** Create a SparseVector by element-wise multiple another SparseVector */
  def *(that: SparseVec[T]): SparseVector[T] = {
    val ks1 = this.activeIndexes.toSet
    val ks2 = that.activeIndexes.toSet
    val kvs: Seq[(Int, T)] = ks1.intersect(ks2).map { k =>
      (k, this (k) * that(k))
    }.toSeq
    SparseVector(kvs)
  }

  /** Add a SparseVector to self */
  def +=(that: SparseVec[T]): this.type = {
    that.active.foreach {
      case (i, v) => this (i) += v
    }
    this.selfCompact
  }

  /** Subtract a SparseVector from self */
  def -=(that: SparseVec[T]): this.type = {
    that.active.foreach {
      case (i, v) => this (i) -= v
    }
    this.selfCompact
  }

  /** Multiply another SparseVector to self */
  def *=(that: SparseVec[T]): this.type = {
    _kvs.foreach {
      case (i, v) => _kvs(i) *= that(i)
    }
    this
  }
}

object SparseVector extends VecLoader[SparseVector] {

  /** Create a SparseVector from a index-value sequence */
  def apply[T: Numeric : ClassTag](kvs: Seq[(Int, T)]): SparseVector[T] =
    new SparseVector(MuMap(kvs: _*))

  /** Create a SparseVector from a index-value sequence */
  def apply[T: Numeric : ClassTag](kvs: Array[(Int, T)]): SparseVector[T] =
    new SparseVector(MuMap(kvs: _*))

  /** Create a SparseVector from a mutable index-value map */
  def apply[T: Numeric : ClassTag](kvs: Map[Int, T]): SparseVector[T] =
    new SparseVector(MuMap(kvs.toSeq: _*))

  /** Create a SparseVector from a mutable index-value map */
  def apply[T: Numeric : ClassTag](map: MuMap[Int, T]): SparseVector[T] =
    new SparseVector(map)

  /**
    * Create a SparseVector by count the frequency of indexes in a Sequence
    *
    * Example: SparseVector(3, List(1,1,2,)) = SparseVector(3, Seq(1->2, 2->1))
    *
    * @param items  index sequence
    */
  def count(items: TraversableOnce[Int]): SparseVector[Int] = {
    val kvs = Counter(items).toSeq
    SparseVector[Int](kvs)
  }

  /** Create a empty SparseVector with fixed size */
  def empty[T: Numeric : ClassTag]: SparseVector[T] =
    new SparseVector[T]()

  /**
    * Parse a Double SparseVector
    * TODO: add try-catch to tackle bad format string
    *
    * @param s  format:"size i:v,i:v,..."
    */
  def parse(s: String): SparseVector[Double] =
    if (s.isEmpty) {
      SparseVector.empty[Double]
    }
    else {
      val kvs = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toDouble)
      }
      SparseVector(kvs)
    }

  /**
    * Parse a Float SparseVector
    * TODO: add try-catch to tackle bad format string
    *
    * @param s  format:"size i:v,i:v,..."
    */
  def parseFloat(s: String): SparseVector[Float] =
    if (s.isEmpty) {
      SparseVector.empty[Float]
    }
    else {
      val kvs = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toFloat)
      }
      SparseVector(kvs)
    }

  /**
    * Parse a Int SparseVector
    * TODO: add try-catch to tackle bad format string
    *
    * @param s  format:"size i:v,i:v,..."
    */
  def parseInt(s: String): SparseVector[Int] =
    if (s.isEmpty) {
      SparseVector.empty[Int]
    }
    else {
      val kvs = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toInt)
      }
      SparseVector(kvs)
    }

  /**
    * Parse a Double SparseVector
    * TODO: add try-catch to tackle bad format string
    *
    * @param s  format:"size i:v,i:v,..."
    */
  def parseLong(s: String): SparseVector[Long] =
    if (s.isEmpty) {
      SparseVector.empty[Long]
    }
    else {
      val kvs = s.split(",").map { kv =>
        val Array(k, v) = kv.split(":")
        (k.toInt, v.toLong)
      }
      SparseVector(kvs)
    }
}