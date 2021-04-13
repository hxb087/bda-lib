package bda.spark.stat

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Counter the items in a RDD
 */
class Counter {

  /** Counter the frequency of each item in `items` */
  def fit[T: ClassTag](items: RDD[T]): RDD[(T, Int)] =
    items.map(i => (i, 1)).reduceByKey(_ + _)
}

