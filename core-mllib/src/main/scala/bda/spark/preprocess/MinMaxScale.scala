package bda.spark.preprocess

import bda.common.obj.RawPoint
import org.apache.spark.rdd.RDD

/**
  * Scale the values of each feature into [0, 1] by
  * (v - min) / (max - min)
  */
object MinMaxScale {

  /**
    * Transform the values of each features in the data collection
    *
    * @param  data  input data sequence, each record is a feature-value Map.
    * @param  mins  The minimum value of each feature
    * @param  maxs   The maximum value of each feature
    * @return  The scaled data collection
    */
  def apply(data: RDD[RawPoint],
            mins: Map[String, Double],
            maxs: Map[String, Double]): RDD[RawPoint] = {
    val bc_mins = data.context.broadcast(mins)
    val bc_maxs = data.context.broadcast(maxs)
    data.map { rd =>
      val fs = rd.fs.map {
        case (f, v) =>
          val new_v = (v - bc_mins.value(f)) / (bc_maxs.value(f) - bc_mins.value(f) + 1e-10)
          (f, new_v)
      }
      RawPoint(rd.id, rd.label, fs)
    }
  }

  /**
    * Scale the feature values in data using the minimum-maximum feature values
    * computed from the input data.
    * @return  (scaled data, mins, maxs)
    */
  def apply(data: RDD[RawPoint]):
  (RDD[RawPoint], Map[String, Double], Map[String, Double]) = {
    val (mins, maxs) = statMinMax(data)
    val new_data = apply(data, mins, maxs)
    (new_data, mins, maxs)
  }

  /**
    * Statistic the minimum and maximum values of each feature
    * @param data  Input data collection, each record represented by a feature-value Map
    * @return  (mins, maxs)
    */
  def statMinMax(data: RDD[RawPoint]):
  (Map[String, Double], Map[String, Double]) = {
    val mins = data.flatMap { rd =>
      rd.fs.toIterable
    }.reduceByKey(math.min).collect().toMap

    val maxs = data.flatMap { rd =>
      rd.fs.toIterable
    }.reduceByKey(math.max).collect().toMap
    (mins, maxs)
  }
}
