package bda.common.linalg

import bda.common.Logging

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/**
  * The base class for SparseVector
  */
private[linalg] abstract class SparseVec[T: Numeric : ClassTag]
  extends Vec[T] with Logging {

  /** Return the value of element with index i */
  def apply(i: Int): T

  /** Return the number of active (non-zero) values */
  def activeSize: Int = activeIndexes.size

  /** Return true if this SparseVec contains no active elements */
  def empty: Boolean = {
    activeSize == 0
  }

  /** Return the sum of all the elements */
  def sum: T = activeValues.sum

  /** Return the square sum of all the elements */
  def squareSum: T = activeValues.map { x =>
    x * x
  }.sum

  /** Return the maximum non-zero element index */
  def maxActiveIndex: Int = if (empty) -1 else activeIndexes.max

  /** Return the non-zero indexes */
  def activeIndexes: Array[Int]

  /** Return the non-zero values */
  def activeValues: Array[T]

  /** Apply function `f` on each pair of non-zero index and value. */
  def foreachActive(f: (Int, T) => Unit): Unit = active.foreach {
    case (i, v) => f(i, v)
  }

  /** Create a SparseVec by applying `f` to each active index-value pair */
  def mapActive[V: Numeric : ClassTag](f: (Int, T) => V): SparseVec[V]

  /** Create a SparseVec by applying `f` to each active value */
  def mapActiveValues[V: Numeric : ClassTag](f: T => V): SparseVec[V]

  /**
    * Create a SparseVec by normalizing this SparseVec
    *
    * @param p  p=1, sum-1 normalize;
    *           p=2, square-sum-1 normalize.
    */
  def normalize(p: Int = 1): SparseVec[Double] = {
    val s: Double = p match {
      case 1 => this.sum.toDouble()
      case 2 => this.norm
      case _ => throw new IllegalArgumentException(s"Unsupport $p norm normalization")
    }
    require(s != 0.0, "normalization: sum is 0")
    this.mapActiveValues(_.toDouble / s)
  }

  /**
    * Return the string representation of this SparseVec
    * @note kvs are sorted by index in increasing order
    * @return a String with the format:"size k:v,k:v,..."
    */
  override def toString: String = this.active.map {
    case (k, v) => s"$k:$v"
  }.mkString(",")

  /** Create a SparseVec whose values are negative of this SparseVec */
  def unary_-(): SparseVec[T] = this.mapActiveValues(-_)

  def *(x: T): SparseVec[T] = this.mapActiveValues(_ * x)

  /** Create a SparseVec by dividing a constant to all the elements of this */
  def /(x: T): SparseVec[T] = {
    require(x != 0.0)
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        mapActiveValues { v => num.div(v, x) }
      case _ =>
        throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Element-wise equality comparison */
  def equal(that: SparseVec[T]): Boolean =
    (this - that).squareSum.toDouble < 1e-6

  /** Create a new SparseVec by adding another SparseVec */
  def +(that: SparseVec[T]): SparseVec[T]

  /** Create a new SparseVec by subtracting another SparseVec */
  def -(that: SparseVec[T]): SparseVec[T]

  /** Create a new SparseVec by element-wise multiplying another SparseVec */
  def *(that: SparseVec[T]): SparseVec[T]

  /**
    * Create a DenseVector by adding a DenseVector to this
    * @return  A DenseVector with the same size of `that`
    */
  def +(that: DenseVector[T]): DenseVector[T] = {
    val dv = that.clone()
    this.foreachActive {
      case (i, v) =>
        if (i < dv.size)
          dv(i) += v
    }
    dv
  }

  /** Create a DenseVector by adding a DenseVector to this */
  def -(that: DenseVector[T]): DenseVector[T] = {
    val dv = -that
    this.foreachActive {
      case (i, v) =>
        if (i < dv.size)
          dv(i) += v
    }
    dv
  }

  /** Create a new SparseVec by element-wise multiplying a DenseVector */
  def *(that: DenseVector[T]): SparseVec[T] =
    this.mapActive {
      case (i, v) => v * that(i)
    }

  /** Create a new SparseVector by element-wise dividing a DenseVector */
  def /(that: DenseVector[T]): SparseVec[T] = {
    require(that.forall(_ != zero), "Divide a DenseVector with zero")
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        this.mapActive {
          case (i, v) => num.div(v, that(i))
        }
      case _ =>
        throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Dot product with another vector */
  def dot(that: Vec[T]): T = that match {
    case dv: DenseVector[T] => (this * dv).sum
    case sv: SparseVec[T] => (this * sv).sum
  }

  /**
    * Create a DenseVector by dot production with a DenseMatrix.
    *
    * This SparseVec will dot product with each row of a DenseMatrix.
    * @return a DenseVector with the size equals to the row number of `that` DenseMatrix
    */
  def dot(that: DenseMatrix[T]): DenseVector[T] = {
    val vec = that.mapRow(this dot _)
    new DenseVector(vec)
  }

  /**
    * Create a DenseVector, where each element is the dot product of this
    * SparseVector with the corresponding row of a SparseMatrix
    *
    * @return a DenseVector with the size equals to the row number of `that` SparseMatrix
    */
  def dot(that: SparseMat[T]): DenseVector[T] = {
    val vec = that.mapRow(_ dot this)
    new DenseVector(vec)
  }

  /** Convert into a Double type SparseVec */
  def toDouble: SparseVec[Double]

  /** Convert into a DenseVector */
  def toDenseVector(size: Int): DenseVector[T] = {
    val vs = Array.fill(size)(zero)
    this.foreachActive {
      case (i, v) => vs(i) = v
    }
    new DenseVector(vs)
  }
}
