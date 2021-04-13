package bda.spark.preprocess

import org.apache.spark.rdd.RDD

import scala.util.Random

object FileSplit {

  /**
   * Split a file into two partitions.
   *
   * @param f A file.
   * @param rate the ratio in two partitions.
   * @return (file_part1, file_part2)
   */
  def apply(f: RDD[String], rate: Double): (RDD[String], RDD[String]) = {
    val Array(f1, f2) = f.randomSplit(Array(rate, (1.0 - rate)), Random.nextLong())
    (f1, f2)
  }
}
