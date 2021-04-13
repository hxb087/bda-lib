package bda.spark.preprocess

import bda.common.obj.RawPoint
import org.apache.spark.rdd.RDD

object FeatureMerge {
  /**
    * Merge multiple feature lists of the same records
    *
    * @param rds1  A record sequence, schema: Seq[(rid, fs)]
    * @param rdss  Other record sequences to be combined with `rds1`
    * @return  A new record sequence
    */
  def apply(rds1: RDD[RawPoint],
            rdss: RDD[RawPoint]*): RDD[RawPoint] = {
    var res = rds1
    for (rds <- rdss)
      res = merge(res, rds)
    res
  }

  /** Merge the features of two record sets */
  private def merge(rds1: RDD[RawPoint],
                    rds2: RDD[RawPoint]
                   ): RDD[RawPoint] = {
    val rs1 = rds1.map(rd => (rd.id, (rd.label, rd.fs)))
    val rs2 = rds2.map(rd => (rd.id, (rd.label, rd.fs)))

    rs1.fullOuterJoin(rs2).map {
      case (id, (Some((label1, fs1)), Some((label, fs2)))) =>
        RawPoint(id, label1, fs1 ++ fs2)
      case (id, (Some((label, fs)), None)) => RawPoint(id, label, fs)
      case (id, (None, Some((label, fs)))) => RawPoint(id, label, fs)
    }
  }
}
