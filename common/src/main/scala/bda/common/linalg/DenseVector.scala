package bda.common.linalg

import bda.common.Logging

import scala.Numeric.Implicits._
import scala.reflect.ClassTag
import scala.util.Random

/**
  * Dense vector, which stores all the elements in a Array.
  */
@SerialVersionUID(1L)
class DenseVector[T: Numeric : ClassTag](private val _vs: Array[T])
  extends Vec[T] with Logging {

  def this(vs: Seq[T]) = this(vs.toArray)

  def this(n: Int) = this(new Array[T](n))

  /** Return the `i`th element */
  override def apply(i: Int): T = {
    if (i < -size || i >= size)
      throw new IndexOutOfBoundsException(i + " not in [0, " + size + ")")
    _vs(i)
  }

  /** Set the value of the ith element to be v */
  def update(i: Int, v: T) {
    _vs(i) = v
  }

  /** Copy the values of `that` DenseVector to this */
  def copy(that: DenseVector[T]): Unit = {
    require(size == that.size, "Cannot copy to different size DenseVectors")
    (0 until size).foreach { i =>
      _vs(i) = that(i)
    }
  }

  /** The number of elements */
  val size: Int = _vs.size

  /** Return the sum of all the elements */
  def sum: T = _vs.sum

  /** Return the square sum of all the elements */
  def squareSum: T = _vs.map { x =>
    x * x
  }.sum

  /** Set the values of all elements to zero */
  def clear(): this.type = {
    (0 until size).foreach { i =>
      _vs(i) = zero
    }
    this
  }

  /** Apply `f` to each element */
  def foreach(f: T => Unit) {
    _vs.foreach(f)
  }

  /**
    * Create a new DenseVector using the values of this DenseVector
 *
    * @param n  if n > size, add zeros; else drop the tail elements.
    * @return  A new DenseVector[T] instance with size n
    */
  def resize(n: Int): DenseVector[T] = {
    val arr = new Array[T](n)
    this.values.copyToArray(arr)
    new DenseVector(arr)
  }

  /** Create a new DenseVector by applying `f` to each element */
  def map[V: Numeric : ClassTag](f: T => V): DenseVector[V] =
    new DenseVector(_vs.map(f))

  /** Create a new DenseVector by applying `f` to each element and its index */
  def mapWithIndex[V: Numeric : ClassTag](f: (Int, T) => V): DenseVector[V] =
    new DenseVector(_vs.zipWithIndex.map {
      case (v, i) => f(i, v)
    })

  /** Tests whether a predicate holds for all elements */
  def forall(f: T => Boolean): Boolean =
    _vs.forall(f)

  /** Return the active key-value pairs (i, v) */
  def active: Array[(Int, T)] = (0 until size).zip(_vs).toArray

  /**
    * Return a Array of pairs by combining corresponding
    * elements in this and that DenseVector.
    *
    * @tparam V  Value type of that DenseVector
    * @return  Array[(v1:T, v2:T)]
    */
  def zip[V](that: DenseVector[V]): Array[(T, V)] = {
    require(this.size == that.size,
      s"zip DenseVector(${size}) with another DenseVector(${that.size})")
    _vs.zip(that._vs)
  }

  /**
    * Convert this DenseVector into a Double type DenseVector
 *
    * @return a new DenseVector[Double]
    */
  def toDouble: DenseVector[Double] = this.map(_.toDouble)

  /** Covert into a scala Vector object */
  def toVector: Vector[T] = _vs.toVector

  /** Covert into a scala Vector object */
  def toArray: Array[T] = _vs

  /** Covert into a scala Sequence */
  def toSeq: Seq[T] = _vs.toSeq

  /** Apply function `f` on each pair of index and value. */
  def foreachActive(f: (Int, T) => Unit): Unit = _vs.zipWithIndex.foreach {
    case (v, i) => f(i, v)
  }

  /**
    * Convert this into a immutable SparseVector
 *
    * @return  a new immutable.SparseVector[T]
    */
  def toImmutableSparse: immutable.SparseVector[T] = {
    val (vs, ks) = _vs.zipWithIndex.filter(_._1 != zero).unzip
    immutable.SparseVector(ks, vs)
  }

  /**
    * Convert this into a mutable SparseVector
 *
    * @return a new mutable.SparseVector[T]
    */
  def toMutableSparse: mutable.SparseVector[T] = {
    val kvs = _vs.zipWithIndex.filter(_._1 != zero).map(t => (t._2, t._1))
    mutable.SparseVector(kvs)
  }


  def unary_-() = new DenseVector(_vs.map(-_))

  def +(x: T) = new DenseVector(_vs.map(_ + x))

  def -(x: T) = new DenseVector(_vs.map(_ - x))

  def *(x: T) = new DenseVector(_vs.map(_ * x))

  /**
    * return a new DenseVecotr via element division this DenseVector with x
    *
    * @note only support Double and Float DenseVector.
    *       Int or Long should first convert to Double use [[DenseVector.toDouble]].
    */
  def /(x: T): DenseVector[T] = {
    require(x != zero, "Divide 0")
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        new DenseVector(_vs.map(num.div(_, x)))
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Add a constant `x` to each element */
  def +=(x: T): this.type = selfOp(x)(_ + _)

  /** Subtract a constant `x` to each element */
  def -=(x: T): this.type = selfOp(x)(_ - _)

  /** Multiply a constant `x` to each element */
  def *=(x: T): this.type = selfOp(x)(_ * _)

  protected def selfOp(x: T)(op: (T, T) => T): this.type = {
    for (i <- 0 until _vs.size) {
      _vs(i) = op(_vs(i), x)
    }
    this
  }

  /**
    * Divide a constant `x` to each element
 *
    * @note only support Double and Float DenseVector.
    *       Int or Long should first convert to Double use vec.toDouble.
    */
  def /=(x: T): DenseVector[T] = {
    require(x != zero, "Divide 0")
    implicitly[Numeric[_]] match {
      case num: Fractional[T] =>
        selfOp(x)(num.div)
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Return a new DenseVector via sum-1 normalization */
  def normalize(p: Int = 1): DenseVector[Double] = {
    val that = this.toDouble
    p match {
      case 1 => that / that.sum
      case 2 => that / that.norm
      case _ => throw new IllegalArgumentException(s"Unsupported $p norm normalization")
    }
  }

  /** Return a new DenseVector via exp-sum-1 normalization */
  def expNormalize(): DenseVector[Double] = new DenseVector({
    val expvs = _vs.map { v => math.exp(v.toDouble) }
    val s = expvs.sum
    expvs.map(_ / s)
  })

  /** Element-wise equality comparison */
  def equal(that: DenseVector[T]): Boolean = {
    if (this.size != that.size)
      false
    else {
      this.zip(that).forall {
        case (a, b) => math.abs((a - b).toDouble) < 1.0e-8
      }
    }
  }

  /**
    * Element-wise addition
 *
    * @return A new DenseVector
    */
  def +(that: DenseVector[T]): DenseVector[T] = vectorOp(that)(_ + _)

  /**
    * Element-wise subtract another DenseVector
    *
    * @param that DenseVector to be subtracted
    * @return A new DenseVector
    */
  def -(that: DenseVector[T]): DenseVector[T] = vectorOp(that)(_ - _)

  /**
    * Element-wise product with another DenseVector
 *
    * @return  A new DenseVector with each element is the product of the two
    *          elements in the corresponding position in the two vectors
    */
  def *(that: DenseVector[T]): DenseVector[T] = vectorOp(that)(_ * _)

  /**
    * Element-size divide
 *
    * @param that  The DenseVector to be divided
    * @return  A new DenseVector
    */
  def /(that: DenseVector[T]): DenseVector[T] = {
    require(that.forall(_ != 0), "Divide a DenseVector with 0")
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        val arr = this.zip(that).map {
          case (a, b) => num.div(a, b)
        }
        new DenseVector(arr)
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Element-wise operation with another DenseVector */
  private def vectorOp(that: DenseVector[T])(op: (T, T) => T): DenseVector[T] = {
    require(this.size == that.size)
    val vs = this.zip(that).map {
      case (a, b) => op(a, b)
    }
    new DenseVector(vs)
  }


  /** Add the corresponding element in `that` DenseVector to this */
  def +=(that: DenseVector[T]): this.type = vectorSelfOp(that)(_ + _)

  /** Subtract the corresponding element in `that` DenseVector from this */
  def -=(that: DenseVector[T]): this.type = vectorSelfOp(that)(_ - _)

  /** Product the corresponding element in `that` DenseVector to this */
  def *=(that: DenseVector[T]): this.type = vectorSelfOp(that)(_ * _)

  /** Perform operation with another DenseVector on this DenseVector */
  private def vectorSelfOp(that: DenseVector[T])(op: (T, T) => T): this.type = {
    require(this.size == that.size)
    for (i <- 0 until _vs.size) {
      _vs(i) = op(_vs(i), that(i))
    }
    this
  }

  /** Dot product between two DenseVectors */
  def dot(that: Vec[T]): T = {
    that match {
      case vec: DenseVector[T] =>
        require(this.size == vec.size)
        _vs.zip(vec._vs).map {
          case (i, j) => i * j
        }.sum
      case vec: SparseVec[T] => (this * vec).sum
    }
  }

  /**
    * Outer product with two DenseVectors.
    *
    * @return a DenseMatrix with dimension this.size * that.size
    */
  def outer(that: DenseVector[T]): DenseMatrix[T] = new DenseMatrix[T]({
    _vs.map { x =>
      that._vs.map { y => x * y }
    }
  })

  /**
    * Create a new DenseVector by adding a SparseVector to this
 *
    * @note Elements not in the DenseVector will be ignored
    * @param that   the SparseVector to be added
    * @return  A DenseVector with the same size of the original DenseVector
    */
  def +(that: SparseVec[T]): DenseVector[T] = {
    val newvs = _vs.clone()
    that.foreachActive {
      case (k, v) =>
        if (k < this.size)
          newvs(k) += v
    }
    new DenseVector(newvs)
  }

  /** Create a new DenseVector by subtracting a SparseVector to this */
  def -(that: SparseVec[T]): DenseVector[T] = {
    val newvs = _vs.clone()
    that.foreachActive {
      case (k, v) =>
        if (k < this.size)
          newvs(k) -= v
    }
    new DenseVector(newvs)
  }

  /** Adding a SparseVector to this */
  def +=(that: SparseVec[T]): this.type = {
    that.foreachActive {
      case (k, v) =>
        if (k < _vs.size)
        _vs(k) += v
    }
    this
  }

  /** Subtracting a SparseVector from this */
  def -=(that: SparseVec[T]): this.type = {
    assert(this.size > that.maxActiveIndex)
    that.foreachActive {
      case (k, v) => _vs(k) -= v
    }
    this
  }

  /** Create a new SparseVector by Element-wise multiplying this and `that` SparseVectors */
  def *(that: SparseVec[T]): SparseVec[T] = {
    that.mapActive {
      case (k, v) =>
        if (k < _vs.size)
           v * _vs(k)
        else zero
    }
  }

  /**
    * Dot product with each row of `that` DenseMatrix.
    * Dimension change: 1xm * nxm  = 1xn
    */
  def dot(that: DenseMatrix[T]): DenseVector[T] = {
    require(that.colNum == this.size,
      s"DenseVector(size=${size}) should have the same number of columns of DenseMatrix(${that.rowNum}, ${that.colNum})")
    val vec = that.mapRow {
      case r: DenseVector[T] => this.dot(r)
    }
    new DenseVector(vec)
  }

  /**
    * Dot product with each row of `that` DenseMatrix.
    * Dimension change: 1xm * nxm  = 1xn
    */
  def dot(that: SparseMat[T]): DenseVector[T] = {
    val vec = that.mapRow {
      case r: SparseVec[T] => this.dot(r)
    }
    new DenseVector(vec)
  }

  /** Create a copy of this DenseVector */
  override def clone(): DenseVector[T] = new DenseVector(_vs.clone())

  /** Return the index-value pairs in increasing order by index */
  def kvs: Array[(Int, T)] = _vs.zipWithIndex.map {
    case (v, k) => (k, v)
  }

  /** Return the value in array form */
  def values: Array[T] = _vs

  /** Convert this DenseVector to a DenseMatrix with dimension 1xsize */
  def toMatrix: DenseMatrix[T] = new DenseMatrix(Array(_vs))

  /** Return the string representation of this DenseVector with format "v,v,.." */
  override def toString = _vs.mkString(",")
}

object DenseVector extends VecLoader[DenseVector] {

  /** Create a DenseVector whose values defined by `elem` */
  def fill[T: Numeric : ClassTag](n: Int)(elem: => T) =
    new DenseVector(Array.fill(n)(elem))

  /** Create a DenseVector whose elements set to zero */
  def zeros[T: Numeric : ClassTag](n: Int) =
    DenseVector.fill(n)(implicitly[Numeric[T]].zero)

  /** Create a DenseVector whose elements set to one */
  def ones[T: Numeric : ClassTag](n: Int) =
    DenseVector.fill(n)(implicitly[Numeric[T]].one)

  /** Create a DenseVector */
  def apply[T: Numeric : ClassTag](vs: T*): DenseVector[T] =
    new DenseVector(vs.toArray)

  /** Create a DenseVector[Int] whose values are frequencies of the indexes in `indexes` */
  def count(size: Int, indexes: TraversableOnce[Int]): DenseVector[Int] = {
    val dv = DenseVector.zeros[Int](size)
    indexes.foreach { i =>
      dv(i) += 1
    }
    dv
  }

  /** Parse a double DenseVector, default is in CSV format */
  def parse(s: String): DenseVector[Double] =
    new DenseVector(s.split(",").map(_.toDouble))

  /** Parse a Float DenseVector, default is in CSV format */
  def parseFloat(s: String): DenseVector[Float] =
    new DenseVector(s.split(",").map(_.toFloat))

  /** Parse a Int DenseVector, default is in CSV format */
  def parseInt(s: String): DenseVector[Int] =
    new DenseVector(s.split(",").map(_.toInt))

  /** Parse a Long DenseVector, default is in CSV format */
  def parseLong(s: String): DenseVector[Long] =
    new DenseVector(s.split(",").map(_.toLong))

  /** Create a Double DenseVector from uniform distribution [0, 1) */
  def rand(n: Int): DenseVector[Double] = new DenseVector(
    Array.fill(n) {
      math.random
    })

  /** Create a Int random vector from uniform distribution [0, K) */
  def randInt(n: Int, k: Int = Int.MaxValue): DenseVector[Int] = new DenseVector(
    Array.fill(n) {
      Random.nextInt(k)
    })

  /** Create a random vector from Gaussian distribution N(0, 1) */
  def randGaussian(n: Int, std: Double = 1.0): DenseVector[Double] = new DenseVector(
    Array.fill(n) {
      Random.nextGaussian() * std
    })
}
