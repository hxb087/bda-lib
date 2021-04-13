package bda.spark.reader

import bda.common.Logging
import bda.common.linalg.immutable.SparseVector
import bda.common.obj.{LabeledPoint, Rate, SVDFeaturePoint}
import bda.common.util.DFUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Read labeledPoints
  */
object Points extends Logging {

  /**
    * Parse LabeledPoint from libSVM format RDD Strings.
    * Each line is a labeled point (libsvm datasets do not have unlabeled data):
    * label fid:v fid:v ...
    *
    * where
    * - Label is {-1, +1} for binary classification, and {0, ..., K-1} for
    * multi-classification.
    * - Fid is start from 1, which should subtract 1 to 0-started.
    * - v is a Double
    *
    * @note User have to specify the feature number ahead
    * @param pt       Input file path
    * @param is_class whether libsvm file is used for classification
    */
  def readLibSVMFile(sc: SparkContext,
                     pt: String,
                     is_class: Boolean): RDD[LabeledPoint] = {
    // parse the LibSVM file
    val rds: RDD[(Double, Array[(Int, Double)])] = sc.textFile(pt).map { ln =>
      val items = ln.split(" ")
      val label = items(0).toDouble

      // decrease fid by 1
      val fvs = items.tail.map { fv =>
        val Array(fid, v) = fv.split(":")
        (fid.toInt - 1, v.toDouble)
      }
      (label, fvs)
    }

    val n_label = rds.map(_._1).distinct().count().toInt
    val n_feature = rds.map(_._2.map(_._1).max).max + 1
    logInfo(s"n(label)=$n_label, n(feature)=$n_feature")

    // transform to labeled points, and adjust label
    rds.zipWithIndex().map { case ((label, fvs), id) =>
      val new_label: Double = if (n_label > 2 && is_class) {
        // for multi-class, decrease label to [0, C-1)
        label - 1
      } else {
        // for binary class
        if (label < 0) 0.0 else label
      }
      val fs = SparseVector(fvs)
      new LabeledPoint(id.toString, new_label, fs)
    }
  }


  def readLibSVMFile(spark: SparkSession,
                     pt: String,
                     is_class: Boolean): RDD[LabeledPoint] = {
    // parse the LibSVM file
    val rds: RDD[(Double, Array[(Int, Double)])] = DFUtils.loadcsv(spark,pt).rdd.map { row =>
      val ln = row.getString(0)
      val items = ln.split(" ")
      val label = items(0).toDouble

      // decrease fid by 1
      val fvs = items.tail.map { fv =>
        val Array(fid, v) = fv.split(":")
        (fid.toInt - 1, v.toDouble)
      }
      (label, fvs)
    }

    val n_label = rds.map(_._1).distinct().count().toInt
    val n_feature = rds.map(_._2.map(_._1).max).max + 1
    logInfo(s"n(label)=$n_label, n(feature)=$n_feature")

    // transform to labeled points, and adjust label
    rds.zipWithIndex().map { case ((label, fvs), id) =>
      val new_label: Double = if (n_label > 2 && is_class) {
        // for multi-class, decrease label to [0, C-1)
        label - 1
      } else {
        // for binary class
        if (label < 0) 0.0 else label
      }
      val fs = SparseVector(fvs)
      new LabeledPoint(id.toString, new_label, fs)
    }
  }
  /**
   * Parse LabeledPoint from libSVM format RDD Strings.
   * Each line is a labeled point (libsvm datasets do not have unlabeled data):
   * label fid:v fid:v ...
   *
   * where
   * - Label is {-1, +1} for binary classification.
   * - Fid is start from 0.
   * - v is a Double.
   *
   * @note User have to specify the feature number ahead
   *
   * @param pt  Input file path
   */
  def readLibFMFile(sc: SparkContext,
                     pt: String): RDD[LabeledPoint] = {
    // parse the LibFM file
    val rds: RDD[(Double, Array[(Int, Double)])] = sc.textFile(pt).map { ln =>
      val items = ln.split(" ")
      val label = items(0).toDouble

      val fvs = items.tail.map { fv =>
        val Array(fid, v) = fv.split(":")
        (fid.toInt, v.toDouble)
      }
      //println(label.toString + fvs.toString)
      (label, fvs)

    }

    val n_label = rds.map(_._1).distinct().count().toInt
    val n_feature = rds.map(_._2.map(_._1).max).max + 1
    logInfo(s"n(label)=$n_label, n(feature)=$n_feature")

    // transform to labeled points, and adjust label
    rds.map { case (label, fvs) =>
      val fs = SparseVector(fvs)
      LabeledPoint(label, fs)
    }
  }


  def readLibFMFile(spark: SparkSession,
                    pt: String): RDD[LabeledPoint] = {
    // parse the LibFM file
    val rds: RDD[(Double, Array[(Int, Double)])] = DFUtils.loadcsv(spark,pt).rdd.map { row =>
      val ln = row.getString(0)
      val items = ln.split(" ")
      val label = items(0).toDouble

      val fvs = items.tail.map { fv =>
        val Array(fid, v) = fv.split(":")
        (fid.toInt, v.toDouble)
      }
      //println(label.toString + fvs.toString)
      (label, fvs)

    }

    val n_label = rds.map(_._1).distinct().count().toInt
    val n_feature = rds.map(_._2.map(_._1).max).max + 1
    logInfo(s"n(label)=$n_label, n(feature)=$n_feature")

    // transform to labeled points, and adjust label
    rds.map { case (label, fvs) =>
      val fs = SparseVector(fvs)
      LabeledPoint(label, fs)
    }
  }
}

object SVDFeaturePoints {

  /**
    * Read SVDFeaturePoints from rates
    * @param pt   format: u   i   v
    */
  def readRatesFile(spark: SparkSession, pt: String): RDD[SVDFeaturePoint] = {
    val df = DFUtils.loadcsv(spark, pt)
    df.rdd.map { ln =>
      val rate = Rate.parse(ln.toString)//此处 编译通过不了 ln==> ln.toString
      val ufs = SparseVector(Seq(rate.user -> 1.0))
      val ifs = SparseVector(Seq(rate.item -> 1.0))
      val gfs = SparseVector.empty[Double](0)
      new SVDFeaturePoint(rate.user + " " + rate.item, rate.label, ufs, ifs, gfs)
    }
  }
}