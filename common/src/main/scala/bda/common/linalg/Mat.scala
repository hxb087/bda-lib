package bda.common.linalg

import bda.common.util.io
import bda.common.util.io.{readLines, writeLines}

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/**
  * Base abstraction for Matrix.
  *
  * In machine learning, matrix has two characters:
  * 1) row operation(read, write), such doc, user, etc.
  * 2) each row might be sparse, but has no empty row (if there is, it
  * can be remove from the training set).
  * Hence, we develop the Matrix as row-based. Each row is store in a
  * Vector, since Vector is covariant and invariant in Scala.
  */
private[linalg] abstract class Mat[T: Numeric : ClassTag](protected val _mat: Vector[Vec[T]])
  extends Serializable {

  protected val zero = implicitly[Numeric[T]].zero
  protected val one = implicitly[Numeric[T]].one

  /** Return the number of rows */
  def rowNum: Int = _mat.size

  /** Return the ith row */
  def apply(i: Int): Vec[T]

  /** Return the element M(i,j) in this matrix M */
  def apply(i: Int, j: Int): T = _mat(i)(j)

  /** Return the maximum element */
  def maxValue: T = _mat.map(_.maxValue).max

  /** Return the minimum element */
  def minValue: T = _mat.map(_.minValue).min

  /** Return the sum of all the elements */
  def sum: T = _mat.map(_.sum).sum

  /** Return the square-sum of all the elements */
  def squareSum: T = _mat.map(_.squareSum).sum

  /** Return the sum of each row */
  def rowSum: DenseVector[T] = new DenseVector(_mat.map(_.sum))

  /** Return the Frobenius norm of this matrix */
  def norm: Double = math.sqrt(squareSum.toDouble)

  /** Return the ith row */
  def row(i: Int): Vec[T]

  /** Return the ith column */
  def col(i: Int): DenseVector[T]

  /** Return the rows */
  def rows: Vector[Vec[T]]

  /** apply f to each active element */
  def foreachActive(f: (Int, Int, T) => Unit): Unit

  /** Generate a new Mat by applying a function over the active (i,j,v) tuples */
  def mapActive[V: Numeric : ClassTag](f: (Int, Int, T) => V): Mat[V]

  /** Return the active (i, j, v) triples in the matrix */
  def active: Seq[(Int, Int, T)] =
    _mat.zipWithIndex.flatMap {
      case (vec, i) => vec.active.map {
        case (j, v) => (i, j, v)
      }
    }

  /**
    * Normalize this matrix to sum-1 of p-norm
    *
    * @param p   p=1, sum-1; p=2, square-sum-1.
    * @return  A Double DenseMatrix
    */
  def normalize(p: Int = 1): Mat[Double]

  /**
    * Normalize each row of this matrix to sum-1 of p-norm
    *
    * @param p   p=1, sum-1; p=2, square-sum-1.
    * @return  A Double DenseMatrix
    */
  def normalizeRow(p: Int = 1): Mat[Double]

  /** Create a new SparseMatrix using the negative of existing values */
  def unary_-(): Mat[T]

  /** Convert this matrix to a Double matrix */
  def toDouble: Mat[Double]

  /** Return the string representation of this matrix */
  override def toString: String = _mat.map(_.toString).mkString("\n")

  /** Save this matrix into file in CSV format */
  def save(pt: String): Unit =
    writeLines(pt, _mat.map(_.toString))
}


/**
  * Trait of Matrix parser and loaders.
  */
private[linalg] trait MatLoader[M[_] <: Mat[_]] {
  /** Create a Double type matrix from a string */
  def parse(lines: Iterator[String]): M[Double]

  /** Create a Float type Matrix from a string */
  def parseFloat(lines: Iterator[String]): M[Float]

  /** Create a Int type matrix from a string */
  def parseInt(lines: Iterator[String]): M[Int]

  /** Create a Long type Matrix from a string */
  def parseLong(lines: Iterator[String]): M[Long]

  /** load a Double matrix from local file */
  def load(pt: String): M[Double] = parse(readLines(pt))

  /** load a Float type matrix from local file */
  def loadFloat(pt: String): M[Float] = parseFloat(readLines(pt))

  /** load a Int type matrix from local file */
  def loadInt(pt: String): M[Int] = parseInt(readLines(pt))

  /** load a Int type matrix from local file */
  def loadLong(pt: String): M[Long] = parseLong(readLines(pt))
}