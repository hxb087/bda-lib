package bda.spark.model.tree.gbrt

import bda.common.linalg.immutable.SparseVector
import bda.common.obj.LabeledPoint
import bda.spark.model.tree.TreeNode
import bda.spark.model.tree.cart.CARTModel
import org.apache.spark.rdd.RDD

/**
  * Case class which stored labels, F-value and features.
  *
  * @param label label of the data point
  * @param f     f-value of the data point
  * @param fs    features of the data point
  */
private[gbrt] case class GBRTPoint(label: Double,
                                   f: Double,
                                   fs: SparseVector[Double]) extends Serializable {

  /**
    * Method to convert the point to an instance of [[String]].
    *
    * @return an instance of [[String]] which represent the point
    */
  override def toString = {
    s"label($label),f($f),fs($fs)"
  }
}

/**
  * Static methods of [[GBRTPoint]].
  */
private[gbrt] object GBRTPoint {

  /**
    * Method to convert data set to a RDD of [[GBRTPoint]].
    *
    * @param lps       data set which represented as [[GBRTPoint]]
    * @return a RDD of [[GBRTPoint]]
    */
  def toGBRTPoint(lps: RDD[LabeledPoint]): RDD[GBRTPoint] = {
    lps.map {
      lp =>
        GBRTPoint(lp.label, 0, lp.fs)
    }
  }

  def update(gbrt_ps: RDD[GBRTPoint], learn_rate: Double, root: TreeNode): RDD[GBRTPoint] = {
    gbrt_ps.mapPartitions {
      iter =>
        iter.map {
          p =>
            val f = p.f + learn_rate * CARTModel.predict(p.fs, root)
            GBRTPoint(p.label, f, p.fs)
        }
    }
  }
}