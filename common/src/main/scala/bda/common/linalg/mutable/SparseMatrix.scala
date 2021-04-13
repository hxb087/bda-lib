package bda.common.linalg.mutable

import bda.common.linalg.{MatLoader, SparseMat, immutable}

import scala.reflect.ClassTag

/**
  * Mutable SparseMatrix, whose values are not allowed to change
  */
class SparseMatrix[T: Numeric : ClassTag](override val _mat: Vector[SparseVector[T]])
  extends SparseMat[T](_mat) {

  selfCompact()

  def this(mat: Seq[SparseVector[T]]) = this(mat.toVector)

  def this(mat: Iterator[SparseVector[T]]) = this(mat.toVector)

  /** Create a M*N empty SparseMatrix */
  def this(M: Int) = this(Vector.fill(M)(new SparseVector[T]()))

  /** Set the value of M(i,j) to `v` */
  def update(i: Int, j: Int, v: T): Unit = {
    _mat(i)(j) = v
  }

  /** Set the elements of `i`th row to the elements in `sv` */
  def copy(i: Int, sv: SparseVector[T]): Unit = {
    _mat(i).copy(sv)
  }

  /** Remove zero-value elements */
  private[linalg] def selfCompact(): this.type = {
    _mat.foreach(_.selfCompact())
    this
  }

  /** Generate a copy of this SparseMatrix */
  override def clone = new SparseMatrix(_mat.map(_.clone))

  /** Reset all the elements to zero **/
  def clear(): this.type = {
    _mat.foreach(_.clear)
    this
  }

  /**
    * Create a new SparseMatrix by p-norm normalization of each row of this matrix
    *
    * @param p  p=1, sum-1 normalization; p=2, square-sum-1 normalization.
    * @return  A normalized SparseMatrix
    */
  def normalizeRow(p: Int = 1): SparseMatrix[Double] = {
    val mat: Vector[SparseVector[Double]] = _mat.map { dv =>
      dv.normalize(p).asInstanceOf[SparseVector[Double]]
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMatrix by applying a function over each active index and value */
  def mapActive[V: Numeric : ClassTag](f: (Int, Int, T) => V): SparseMatrix[V] = {
    val mat = _mat.zipWithIndex.map {
      case (sv, i) => sv.mapActive {
        case (j, v) => f(i, j, v)
      }
    }
    new SparseMatrix(mat)
  }

  /** Apply f to each active element */
  def foreachActive(f: (Int, Int, T) => Unit): Unit = {
    _mat.zipWithIndex.foreach {
      case (sv, i) => sv.foreachActive {
        case (j, v) => f(i, j, v)
      }
    }
  }

  /** Create a new SparseMatrix by applying a function over each active value */
  def mapActiveValues[V: Numeric : ClassTag](f: T => V): SparseMatrix[V] = {
    val mat = _mat.map(_.mapActiveValues(f))
    new SparseMatrix(mat)
  }

  /** Convert to a Double matrix */
  def toDouble: SparseMatrix[Double] = {
    val mat: Vector[SparseVector[Double]] = _mat.map(_.toDouble)
    new SparseMatrix(mat)
  }

  /** Convert into a Vector of SparseVector */
  def toVector: Vector[SparseVector[T]] = _mat

  /** Convert this to a immutable SparseMatrix */
  def toImmutable: immutable.SparseMatrix[T] = {
    val mat: Vector[immutable.SparseVector[T]] = _mat.map(_.toImmutable)
    new immutable.SparseMatrix(mat)
  }

  /** Create a new SparseMat by adding another SparseMat */
  def +(that: SparseMat[T]): SparseMatrix[T] = {
    val mat = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 + sv2
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMat by subtracting another SparseMat */
  def -(that: SparseMat[T]): SparseMatrix[T] = {
    val mat = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 - sv2
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMat by element-wise multiply another SparseMat */
  def *(that: SparseMat[T]): SparseMatrix[T] = {
    val mat: Vector[SparseVector[T]] = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 * sv2
    }
    new SparseMatrix(mat)
  }

  /** Add a SparseMatrix to this Matrix */
  def +=(that: SparseMat[T]): this.type = {
    _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 += sv2
    }
    this.selfCompact()
  }

  /** Subtract a SparseMatrix from this Matrix */
  def -=(that: SparseMat[T]): this.type = {
    _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 -= sv2
    }
    this.selfCompact()
  }

  /** Element-product a SparseMatrix to this Matrix */
  def *=(that: SparseMat[T]): this.type = {
    _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 *= sv2
    }
    this
  }
}

object SparseMatrix extends MatLoader[SparseMatrix] {

  /** Parse a Double SparseMatrix from strings */
  def parse(lines: Iterator[String]): SparseMatrix[Double] =
    new SparseMatrix(lines.map(SparseVector.parse))

  /** Parse a Float SparseMatrix from strings */
  def parseFloat(lines: Iterator[String]): SparseMatrix[Float] =
    new SparseMatrix(lines.map(SparseVector.parseFloat))

  /** Parse a Int matrix from strings */
  def parseInt(lines: Iterator[String]): SparseMatrix[Int] =
    new SparseMatrix(lines.map(SparseVector.parseInt))

  /** Parse a Int matrix from strings */
  def parseLong(lines: Iterator[String]): SparseMatrix[Long] =
    new SparseMatrix(lines.map(SparseVector.parseLong))

  /**
    * Create a SparseMatrix from (rowID, colID, v) triples
 *
    * @param m  row number
    * @param triples  (rowID, colID, value)
    * @tparam T  value type
    * @return  A immutable m*n SparseMatrix
    */
  def fromTriples[T: Numeric : ClassTag](m: Int,
                                         triples: TraversableOnce[(Int, Int, T)]
                                        ): SparseMatrix[T] = {
    val mat = new SparseMatrix[T](m)
    triples.foreach {
      case (i, j, v) => mat(i, j) = v
    }
    mat
  }

  /**
    * Create a SparseMatrix from (rowID, colID, v) triples
    *
    * @param triples  (rowID, colID, value)
    * @tparam T  value type
    * @return  A immutable m SparseMatrix whose dimensions are determined by
    *          the maximum rowID and colID in the triples.
    */
  def fromTriples[T: Numeric : ClassTag](triples: Iterable[(Int, Int, T)]):
  SparseMatrix[T] = {
    val m = triples.map(_._1).max + 1
    fromTriples(m, triples)
  }
}