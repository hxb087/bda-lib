package bda.common.util

import bda.common.linalg.DenseVector

import scala.reflect.ClassTag
import scala.util.Random

/**
 * Sampling function collection
 */
object Sampler {
  /** Draw a Int value uniformly form [0, K) */
  def uniform(K: Int): Int = Random.nextInt(K)

  /** Draw a Double value uniformly form [0, 1) */
  def uniform(): Double = Random.nextDouble

  /**
    * Draw a binary value from a Bernoulli distribution.
   *
    * @param p  the probability of draw 1
    * @return 0 or 1
    */
  def Bernoulli(p: Double = 0.5): Int = if (Random.nextDouble() < p) 1 else 0

  /**
   * Draw a Int value from a Multinomial distribution.
   *
   * @param p   [P(0), P(1), ..., P(k-1)]
   * @return  a Int in [0, K-1]
   */
  def multinomial(p: Array[Double]): Int = {
    val arr = p.clone()
    for (i <- 1 until p.size)
      arr(i) += arr(i - 1)

    val u = Random.nextDouble
    var k = 0
    while (k < p.size && arr(k) < u * arr(p.size - 1))
      k += 1
    if (k == p.size) k - 1 else k
  }

  /**
   * Draw a Int value from a Multinomial distribution.
   *
   * @param p [P(0), P(1), ..., P(k-1)]
   * @return a Int in [0, K - 1]
   */
  def multinomial(p: DenseVector[Double]): Int =
    multinomial(p.toArray)

  /**
   * Draw a Int value from a Multinomial distribution.
   *
   * @param p [P(0), P(1), ..., P(k-1)]
   * @return a Int in [0, K - 1]
   */
  def multinomial(p: Seq[Double]): Int =
    multinomial(p.toArray)

  /**
   * Sample the elements in the array, and return the sub samples.
   *
   * @param arr all elements stored in Array.
   * @param rate sample ratio.
   * @tparam T type of elements.
   * @return sub samples of the sequence.
   */
  def subSample[T: ClassTag](arr: Array[T], rate: Double): Array[T] = {

    val tol = arr.length
    val num = (tol * rate).ceil.toInt
    val ans = arr.clone()

    Range(num, tol).foreach {
      id =>
        val rnd = uniform(id + 1)

        val tmp = ans(id)
        ans(id) = ans(rnd)
        ans(rnd) = tmp
    }

    ans.slice(0, num)
  }

  /**
   * Sample the elements from the sequence [0, 1, 2, ... k-1].
   *
   * @param tol k, number of the universal set.
   * @param rate sample ratio.
   * @return sub samples of the sequence.
   */
  def subSample(tol: Int, rate: Double): Array[Int] = {

    val num = (tol * rate).ceil.toInt
    val arr = Array.tabulate(tol)(index => index)

    subSample[Int](arr, rate)
  }
}