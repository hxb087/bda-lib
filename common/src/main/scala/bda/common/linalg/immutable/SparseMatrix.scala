package bda.common.linalg.immutable

import bda.common.linalg.mutable.{SparseMatrix => MSM}
import bda.common.linalg.{MatLoader, SparseMat}

import scala.reflect.ClassTag

/**
  * Immutable SparseMatrix, whose values are not allowed to change
  */
class SparseMatrix[T: Numeric : ClassTag](override val _mat: Vector[SparseVector[T]])
  extends SparseMat[T](_mat) {

  def this(mat: Seq[SparseVector[T]]) = this(mat.toVector)

  def this(mat: Iterator[SparseVector[T]]) = this(mat.toVector)

  /**
    * P-norm normalization for each Row
 *
    * @param p   p=1, sum-1; p=2, square-sum-1.
    * @return  A Double DenseMatrix
    */
  override def normalizeRow(p: Int = 1): SparseMatrix[Double] = {
    val mat: Vector[SparseVector[Double]] = _mat.map { dv =>
      dv.normalize(p).asInstanceOf[SparseVector[Double]]
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMatrix by applying a function over
    * each active index and value */
  override def mapActive[V: Numeric : ClassTag](f: (Int, Int, T) => V
                                               ): SparseMatrix[V] = {
    val mat = _mat.zipWithIndex.map {
      case (sv, i) => sv.mapActive {
        case (j, v) => f(i, j, v)
      }
    }
    new SparseMatrix(mat)
  }

  /** Apply f to each active element */
  override def foreachActive(f: (Int, Int, T) => Unit): Unit =
    _mat.zipWithIndex.foreach {
      case (sv, i) => sv.foreachActive {
        case (j, v) => f(i, j, v)
      }
    }

  /** Create a new SparseMatrix by applying a function over each active value */
  override def mapActiveValues[V: Numeric : ClassTag](f: T => V
                                                     ): SparseMatrix[V] = {
    val mat = _mat.map(_.mapActiveValues(f))
    new SparseMatrix(mat)
  }

  /** Convert to a Double matrix */
  override def toDouble: SparseMatrix[Double] =
    new SparseMatrix(_mat.map(_.toDouble))

  /** Convert into a Vector of SparseVector */
  def toVector: Vector[SparseVector[T]] = _mat

  /** Create a new SparseMat by adding another SparseMat */
  override def +(that: SparseMat[T]): SparseMatrix[T] = {
    val mat = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 + sv2
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMat by subtracting another SparseMat */
  override def -(that: SparseMat[T]): SparseMatrix[T] = {
    val mat = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 - sv2
    }
    new SparseMatrix(mat)
  }

  /** Create a new SparseMat by element-wise multiplying another SparseMat */
  override def *(that: SparseMat[T]): SparseMatrix[T] = {
    val mat = _mat.zip(that._mat).map {
      case (sv1, sv2) => sv1 * sv2
    }
    new SparseMatrix(mat)
  }
}

object SparseMatrix extends MatLoader[SparseMatrix] {

  /** parse a Double SparseMatrix from strings */
  def parse(lines: Iterator[String]): SparseMatrix[Double] =
    new SparseMatrix(lines.map(SparseVector.parse))

  /** parse a Float SparseMatrix from strings */
  def parseFloat(lines: Iterator[String]): SparseMatrix[Float] =
    new SparseMatrix(lines.map(SparseVector.parseFloat))

  /** parse a Int matrix from strings */
  def parseInt(lines: Iterator[String]): SparseMatrix[Int] =
    new SparseMatrix(lines.map(SparseVector.parseInt))

  /** parse a Int matrix from strings */
  def parseLong(lines: Iterator[String]): SparseMatrix[Long] =
    new SparseMatrix(lines.map(SparseVector.parseLong))

  /**
    * Create a SparseMatrix from (rowID, colID, v) triples
 *
    * @param m  row number
    * @param n  column number
    * @param triples  (rowID, colID, value)
    * @tparam T  value type
    * @return  a immutable m*n SparseMatrix
    */
  def fromTriples[T: Numeric : ClassTag](m: Int,
                                         triples: TraversableOnce[(Int, Int, T)]
                                        ): SparseMatrix[T] = {
    val mat = new MSM[T](m)
    triples.foreach {
      case (i, j, v) => mat(i, j) = v
    }
    mat.toImmutable
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