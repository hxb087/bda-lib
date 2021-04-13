package bda.common.obj

import bda.common.linalg.immutable.SparseVector
import bda.common.util.MapUtil
import org.apache.spark.sql.Row

case class RawPoint(id: String,
                    label: Double,
                    fs: Map[String, Double]) {

  /** Number of non-zero features in the RawPoint */
  def activeFeatureNum: Int = fs.size

  override def toString =
    s"$id\t$label\t${MapUtil.str(fs)}"
}

/** Factory methods for RawPoint */
object RawPoint {

  val default_label = Double.NaN

  val default_id = ""

  def apply(fs: Map[String, Double]): RawPoint =
    new RawPoint(default_id, default_label, fs)

  def apply(id: String, fs: Map[String, Double]): RawPoint =
    new RawPoint(id, default_label, fs)

  def apply(label: Double, fs: Map[String, Double]): RawPoint =
    new RawPoint(default_id, label, fs)

  /**
   * Parse a Raw point
   *
   * @param s
   * @return
   */
  def parse(s: String): RawPoint = s.split("\t", 3) match {
    case Array(id, label, fv_s) =>
      //返回map
      val fv = MapUtil.parse(fv_s).map {
        case (k, v) => (k, v.toDouble)
      }
      RawPoint(id, label.toDouble, fv)

    case Array(label, fv_s) =>
      val fv = MapUtil.parse(fv_s).map {
        case (k, v) => (k, v.toDouble)
      }
      RawPoint(label.toDouble, fv)

    case Array(fv_s) =>
      val fv = MapUtil.parse(fv_s).map {
        case (k, v) => (k, v.toDouble)
      }
      RawPoint(fv)

    case _ => throw new RuntimeException("failed to parse RawPoint from:" + s)
  }
}

/**
 * Point with Sparse features
 */
case class LabeledPoint(id: String,
                        label: Double,
                        fs: SparseVector[Double]) {

  /** Return the number of active features in this Point */
  def activeFeatureNum: Int = fs.activeSize

  override def toString =
    s"$id\t$label\t$fs"
}

/** Factory of Point */
object LabeledPoint {

  val default_label = Double.NaN

  val default_id = "0"

  def apply(fs: SparseVector[Double]): LabeledPoint =
    new LabeledPoint(default_id, default_label, fs)

  def apply(label: Double, fs: SparseVector[Double]): LabeledPoint =
    new LabeledPoint(default_id, label, fs)

  def apply(id: String, fs: SparseVector[Double]): LabeledPoint =
    new LabeledPoint(id, default_label, fs)

  /**
   * Parse a unlabeled or labeled Point from a string
   *
   * @param s format of a point with id: "id    label   f:v ...", or
   *          without id: "label   f:v ..."
   */
  def parse(s: String): LabeledPoint = s.split("\t", 3) match {
    case Array(id, label, fvs) =>
      val fv = SparseVector.parse(fvs)
      LabeledPoint(id, label.toDouble, fv)

    case Array(label, fvs) =>
      val fv = SparseVector.parse(fvs)
      LabeledPoint(label.toDouble, fv)

    case Array(fvs) =>
      val fv = SparseVector.parse(fvs)
      LabeledPoint(fv)

    case _ => throw new RuntimeException(s"failed to parse LabeledPoint from: $s")
  }
}

case class Rate(user: Int, item: Int, label: Double) {

  override def toString: String = s"$label\t$user\t$item"
}

object Rate {
  val default_label = 0.0

  /** Create a Rate instance with the default label 0.0 */
  def apply(user: Int, item: Int): Rate = Rate(user, item, default_label)

  /**
   * Parse a Rate from string
   *
   * @param s if with label, format is: "label   user   item"
   *          if without lable, format is: "user   item"
   */
  def parse(s: String): Rate = s.split("\\s+") match {
    case Array(r, u, i) => Rate(u.toInt, i.toInt, r.toDouble)
    case Array(u, i) => Rate(u.toInt, i.toInt)
    case _ => throw new IllegalArgumentException(s"Cannot parse Rate from: $s")
  }

  def parse(row: Row): Rate = row.length match {
    case 3 => Rate(row.get(1).toString.toInt, row.get(2).toString.toInt, row.get(0).toString.toDouble)
    case 0 => Rate(row.get(0).toString.toInt, row.get(1).toString.toInt)
    case _ => throw new IllegalArgumentException(s"Cannot parse Rate from: ${row.toString()}")
  }
}

/**
 * Point for SVDFeature
 *
 * @param label label
 * @param u_fs  user features
 * @param i_fs  item features
 * @param g_fs  global features]\['?';/,cxzz
 */
case class SVDFeaturePoint(id: String,
                           label: Double,
                           u_fs: SparseVector[Double],
                           i_fs: SparseVector[Double],
                           g_fs: SparseVector[Double]) {

  /** Create a SVDFeaturePoint without global features */
  def this(id: String, label: Double, u_fs: SparseVector[Double], i_fs: SparseVector[Double]) =
    this(id, label, u_fs, i_fs, SparseVector.empty[Double](0))

  /**
   * Generate a string representation of SVDFeaturePoint
   *
   * @return "label    fu:v fu:v,...,fi:v fi:v...,fg:v fg:v,..."
   */
  override def toString: String = s"$id\t$label\t${u_fs},${i_fs},${g_fs}"
}

object SVDFeaturePoint {

  val default_label = 0.0
  val default_id = "0"

  def apply(u_fs: SparseVector[Double],
            i_fs: SparseVector[Double],
            g_fs: SparseVector[Double]): Unit =
    SVDFeaturePoint(default_id, default_label, u_fs, i_fs, g_fs)

  /**
   * Parse a SVDFeaturePoint from a string. For unlabeled point, its
   * label is Double.NaN.
   *
   * @param s format: "label    fu:v fu:v,...,fi:v fi:v...,fg:v fg:v,...". Note:
   *          SVDFeature only support binary classification with label {-1, +1}
   * @return
   */
  def parse(s: String): SVDFeaturePoint = s.split("\t") match {
    case Array(id, label, fs) =>
      val (u_fs, i_fs, g_fs) = parseFeatureVectors(fs)
      new SVDFeaturePoint(id, label.toDouble, u_fs, i_fs, g_fs)
    case Array(label, fs) =>
      val (u_fs, i_fs, g_fs) = parseFeatureVectors(fs)
      new SVDFeaturePoint(default_id, label.toDouble, u_fs, i_fs, g_fs)

    case Array(fs) =>
      val (u_fs, i_fs, g_fs) = parseFeatureVectors(fs)
      new SVDFeaturePoint(default_id, Double.NaN, u_fs, i_fs, g_fs)

    case other => throw new IllegalArgumentException("Cannot parse SVDFeaturePoint from $s")
  }

  /**
   * Parse the user, item, global feature vectors
   *
   * @param s format: fu:v fu:v,fi:v fi:v...,fg:v fg:v ...
   * @return (user_fvs, item_fvs, global_fvs)
   */
  private def parseFeatureVectors(s: String):
  (SparseVector[Double], SparseVector[Double], SparseVector[Double]) =
    s.split(",") match {
      case Array(su, si, sg) =>
        val u_fs = SparseVector.parse(su)
        val i_fs = SparseVector.parse(si)
        val g_fs = SparseVector.parse(sg)
        (u_fs, i_fs, g_fs)

      case Array(su, si) =>
        val u_fs = SparseVector.parse(su)
        val i_fs = SparseVector.parse(si)
        (u_fs, i_fs, SparseVector.empty[Double](0))

      case other =>
        throw new IllegalArgumentException("Failed parse user-item-global feature vectors from $s")
    }
}

