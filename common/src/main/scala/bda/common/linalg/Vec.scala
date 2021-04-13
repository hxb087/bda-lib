package bda.common.linalg

import bda.common.util.io
import bda.common.util.io.{readLines, writeLines}

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/** Basic Vector Abstraction */
private[linalg] abstract class Vec[T: Numeric : ClassTag] extends Serializable {

  protected val zero = implicitly[Numeric[T]].zero

  def apply(i: Int): T

  /** The sum of all the elements */
  def sum: T

  /** The square sum of all the elements */
  def squareSum: T

  /**
   * The minimum of all the elements
   * @return (index, value)
   */
  def min: (Int, T) = active.minBy(_._2)

  /**
   * The maximum of all the elements
   * @return (index, value)
   */
  def max: (Int, T) = active.maxBy(_._2)

  /** Return the minimum value */
  def minValue: T = min._2

  /** Return the maximal value */
  def maxValue: T = max._2

  /** Return the index of minimum element */
  def argmin: Int = min._1

  /** Return the index of maximum element */
  def argmax: Int = max._1

  /** Return the largest `n` indexes and values */
  def top(n: Int): Array[(Int, T)] = active.sortBy { x =>
    -x._2
  }.take(n)

  /** Return the indexes of the largest `n` elements */
  def argtop(n: Int): Array[Int] = top(n).map(_._1)

  /** Return the values of the largest `n` elements */
  def topValues(n: Int): Array[T] = top(n).map(_._2)

  /** Return the l2 norm of this Vector */
  def norm: Double = math.sqrt(squareSum.toDouble)

  /** Return the inner product of this and that vector */
  def dot(that: Vec[T]): T

  /** Return the cosine similarity of this and that vector */
  def cos(that: Vec[T]): Double = dot(that).toDouble() / (norm * that.norm)

  /** Convert into a Double type Vec */
  def toDouble: Vec[Double]

  /** Apply function `f` on each pair of non-zero index and value. */
  def foreachActive(f: (Int, T) => Unit): Unit

  /** Return the active key-value pairs (i, v) */
  def active: Array[(Int, T)]

  /**
   * Create a Vec by normalizing this one
   * @param p  p=1, sum-1 normalize, i.e., the sum of the result Vector is 1.
   *           p=2, square-sum-1 normalize, i.e., the sqaure of the result Vector is 1.
   */
  def normalize(p: Int = 1): Vec[Double]

  /** Write the vector into a file. Each line is a value. */
  def save(pt: String): Unit =
    writeLines(pt, Iterator(this.toString))
}

/**
 * Abstraction of Vector loader
 */
private[linalg] trait VecLoader[M[_] <: Vec[_]] {
  /** Parse a Double type Vec from a string */
  def parse(s: String): M[Double]

  /** Parse a Float type Vec from a string */
  def parseFloat(s: String): M[Float]

  /** Parse a Int type Vec from a string */
  def parseInt(s: String): M[Int]

  /** Parse a Long type Vec from a string */
  def parseLong(s: String): M[Long]

  /** Load a Double type Vec from a file */
  def load(pt: String): M[Double] = parse(readLines(pt).next.trim)

  /** Load a Float type Vec from a file */
  def loadFloat(pt: String): M[Float] = parseFloat(readLines(pt).next.trim)

  /** Load a Int type Vec from a file */
  def loadInt(pt: String): M[Int] = parseInt(readLines(pt).next.trim)

  /** Load a Long type Vec from a file */
  def loadLong(pt: String): M[Long] = parseLong(readLines(pt).next.trim)
}