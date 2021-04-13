package bda.spark.preprocess

import bda.common.linalg.immutable.SparseVector
import bda.common.obj.{LabeledPoint, RawPoint}
import org.apache.spark.rdd.RDD

/**
  * Index string features to feature ids that starts from 0.
  * @note feature id are Int, not Long, since the index of
  *       SparseVector are Int.
  */
object FeatureIndex {

  /**
    * Transform features from string to indexes in records with
    * a existing feature index Map
    * @param rds    Input records
    * @param dict   A map from feature name to its index
    * @return  Records represented by SparseVector
    */
  def apply(rds: RDD[RawPoint],
            dict: Map[String, Int]): RDD[LabeledPoint] = {
    val bc_dict = rds.context.broadcast(dict)
    rds.map { rd =>
      val kvs: Map[Int, Double] = rd.fs.filter {
        case (k, v) => bc_dict.value.contains(k)
      }.map {
        case (k, v) => (bc_dict.value(k), v)
      }
      val fs = SparseVector(kvs)
      LabeledPoint(rd.id, rd.label, fs)
    }
  }

  /** Build a new dictionary by fitting `rds` and then transform features from string to Int in `rds` */
  def apply(rds: RDD[RawPoint]):
  (RDD[LabeledPoint], Map[String, Int]) = {
    val dict = fit(rds)
    val new_rds = apply(rds, dict)
    (new_rds, dict)
  }

  /** Build a new dictionary by fitting records in the format of "feature:v feature:v ..." */
  def fit(rds: RDD[RawPoint]): Map[String, Int] =
    rds.flatMap { rd =>
      rd.fs.keys
    }.distinct.collect().zipWithIndex.toMap
}
