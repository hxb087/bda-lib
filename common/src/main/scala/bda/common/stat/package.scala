package bda.common

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/**
  * Utilities for statistics
  */
package object stat {

  /** Compute the mean of a Iterable collection */
  def mean[T: Numeric](vs: Iterable[T]): Double =
    vs.sum.toDouble / vs.size.toDouble

  def findKMedian[T: Numeric](arr: Array[T], k: Int): T = {

    val a = arr(scala.util.Random.nextInt(arr.size))
    val (s, b) = arr.partition(implicitly[Numeric[T]].gt(a, _))
    if (s.size == k) a
    // The following test is used to avoid infinite repetition
    else if (s.isEmpty) {
      val (s, b) = arr partition (a ==)
      if (s.size > k) a
      else findKMedian(b, k - s.size)
    } else if (s.size < k) findKMedian(b, k - s.size)
    else findKMedian(s, k)
  }

  /**
    * Find the median of a Iterable collection.
    * Return the smaller one if there are two candidates.
    */
  def median[T: Numeric](arr: Array[T]): T = findKMedian(arr, (arr.size - 1) / 2)

  def median[T : Numeric : ClassTag](vs: Iterable[T]): T = median(vs.toArray)

  /** Compute the variance of a Iterable collection */
  def variance[T: Numeric](vs: Iterable[T]): Double = {
    val m = mean(vs)
    vs.map(v => v * v).sum.toDouble / vs.size.toDouble - m * m
  }

  /** Compute the standard variance of a Iterable collection */
  def stddev[T: Numeric](vs: Iterable[T]): Double =
    math.sqrt(variance(vs))
}
