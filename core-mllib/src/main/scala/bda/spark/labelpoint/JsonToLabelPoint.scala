package bda.spark.labelpoint

import bda.common.linalg.DenseVector
import bda.common.obj.LabeledPoint
import bda.common.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * This part transform the json data into labelpoint.
 */
object JsonToLabelPoint extends Logging {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("JsonToLabelPoint") {
      head("JsonToLabelPoint", "1.0")
      opt[String]("input_pt").required()
        .text("input_pt is the path stores the json file.")
        .action { (x, c) => c.copy(input_pt = x) }
      opt[String]("output_pt").required()
        .text(("output_pt is the path stores the labelpoint file"))
        .action { (x, c) => c.copy(output_pt = x) }
      opt[String]("label_col").required()
        .text("label_col is the column which is treated as the label")
        .action { (x, c) => c.copy(label_col = x) }
      opt[Boolean]("is_class").required()
        .text("identify whether the json file is used for classification.")
        .action { (x, c) => c.copy(is_class = x) }
      help("help").text("prints this usage text")
    }
    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(para: Params): Unit = {
    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("JsonToLabelPoint")
      .getOrCreate()

    val tableDF = spark.read.json(para.input_pt)
    transformToLabelPoint(tableDF, para.label_col, para.is_class, para.output_pt)
  }

  /**
   * Transform a dataframe into labelpoint file.
   * Mainly used for classification and regression training files.
   * Attention: As for multi-class task, the label starts from [0, C) C is the total classes.
   * As for two-class task, the negative label is 0, the positive label is 1.
   *
   * @param tableDF   dataframe derived from the json file.
   * @param label_col the label column name.
   * @param is_class  whether the file is used for classificaiton.
   * @param output_pt the path to store the labelPoint file.
   */
  private def transformToLabelPoint(tableDF: DataFrame, label_col: String, is_class: Boolean, output_pt: String): Unit = {
    val col_names = tableDF.columns.filter(p => (!p.equals(label_col)))
    val rds = tableDF.rdd.filter(ln => ln.getAs(label_col) != null).map {
      ln =>
        val label = ln.getAs(label_col).toString.toDouble
        val fvs = ArrayBuffer[Double]()
        for (x <- col_names) {
          fvs += ln.getAs(x).toString.toDouble
        }
        (label, fvs)
    }
    val n_label = rds.map(_._1).distinct().count().toInt
    val n_feature = col_names.length

    logInfo(s"n(label)=$n_label, n(feature)=$n_feature")

    //transform to labeledpoints, and adjust label
    rds.zipWithIndex().map {
      case ((label, fvs), id) =>
        val new_label = if (n_label > 2 && is_class) {
          //for multi_class, decrease label to [0, C-1)
          label - 1
        }
        else if (label <= 0 && is_class)
          0.0
        else
          label
        val fs = new DenseVector(fvs).toImmutableSparse
        new LabeledPoint(id.toString, new_label, fs)
    }.map {
      labelPoint =>
        labelPoint.toString
    }.saveAsTextFile(output_pt)
  }
}

case class Params(input_pt: String = "",
                  output_pt: String = "",
                  label_col: String = "",
                  is_class: Boolean = true)
