package bda.spark.model.SVDFeature

import bda.common.linalg.DenseVector
import bda.common.linalg.immutable.SparseVector
import org.apache.spark.rdd.RDD


class SVDFeatureGraphXModel(val b: Double,
                            val wg: DenseVector[Double],
                            val uwz: RDD[(Int, (Double, DenseVector[Double]))],
                            val iwz: RDD[(Int, (Double, DenseVector[Double]))]) {
  require(!uwz.isEmpty() && !iwz.isEmpty())

  val K = uwz.first()._2._2.size

  /**
   * Predict the rating for a single record
 *
   * @param u_fs   user features
   * @param i_fs   item features
   * @param g_fs   global feature-values for (u, i)
   * @return  The predicted rating
   */
  def predict(u_fs: SparseVector[Double],
              i_fs: SparseVector[Double],
              g_fs: SparseVector[Double] = null): Double = {
    // global factor part
    var y = b
    if (g_fs != null && !g_fs.empty)
      y += (wg dot g_fs)

    // user factor part
    val uz = DenseVector.zeros[Double](K)
    u_fs.foreachActive {
      case (f, v) =>
        val res: Seq[(Double, DenseVector[Double])] = uwz.lookup(f)
        if (res.nonEmpty) {
          val (w, zs) = res.head
          y += w * v
          uz += zs
        }
    }

    // item factor part
    val iz = DenseVector.zeros[Double](K)
    i_fs.foreachActive {
      case (f, v) =>
        val res: Seq[(Double, DenseVector[Double])] = iwz.lookup(f)
        if (res.nonEmpty) {
          val (w, zs) = res.head
          y += w * v
          iz += zs
        }
    }
    y += uz dot iz
    y
  }

  /**
   * @todo Predict the rating for a single record
   * @param rds   RDD[(u_fs, i_fs, g_fs)]
   * @return  The predicted rating for each record
   */
  def predict(rds: RDD[(SparseVector[Double], SparseVector[Double], SparseVector[Double])]): RDD[Double] = {
    rds.mapPartitions { p_rds =>
      // collect the required user and item factors from RDD

      p_rds.map {
        case (u_fs, i_fs, g_fs) => var y = 0.0
          y
      }
    }
  }
}

/**
 * Train SVDFeature with GraphX
 */
class SVDFeatureGraphXTrainer {

}
