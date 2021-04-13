package org.apache.spark.ml.stat

import org.apache.spark.ml.linalg.Vector

/**
 * @author ：ljf
 * @date ：2020/9/22 19:00
 * @description：implicit class for statistics
 * @modified By：
 * @version: $ 1.0
 */
object Implicit {
  implicit class NewVector(val vector: Vector) {
    val values = vector.toArray
    val size = values.length

    private[spark] def iterator: Iterator[(Int, Double)] = {
      val localValues = values
      Iterator.tabulate(size)(i => (i, localValues(i)))
    }

    private[spark] def activeIterator: Iterator[(Int, Double)] =
      iterator

    private[spark] def nonZeroIterator: Iterator[(Int, Double)] =
      activeIterator.filter(_._2 != 0)
  }
}
