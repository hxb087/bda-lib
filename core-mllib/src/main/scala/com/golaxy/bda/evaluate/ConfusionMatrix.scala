package com.golaxy.bda.evaluate

import java.util

import breeze.linalg.Axis
import com.databricks.spark.avro.SchemaConverters
import com.golaxy.bda.utils.{PathUtils, DFUtils, AppUtils}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser
import org.apache.spark.sql.functions._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by lijg on 2020/4/16.
  * 混淆矩阵
  * 数据样式：
  * {"labels":0.0,"0.0":24812.0,"1.0":9.0}
  * {"labels":1.0,"0.0":553.0,"1.0":2252.0}
  *
  * 统计数据：
  * 数据样式：
  * {"label":0.0,"trueNum":24812,"falseNum":553,"totalNum":24821,"accuracy":0.979656845001086,"precision":0.9781983047506406,"recall":0.9996374038112888,"f1Score":0.9888016578328618}
  * {"label":1.0,"trueNum":2252,"falseNum":9,"totalNum":2805,"accuracy":0.979656845001086,"precision":0.9960194604157453,"recall":0.8028520499108734,"f1Score":0.8890643505724437}
  */
object ConfusionMatrix {

  case class Params(
                     input_pt: String = "D:\\testdata\\evaluate\\ConfusionMatrix\\input\\confu.csv",
                     labelCol: String = "label",
                     predictCol: String = "predict",
                     output_pt: String = "D:\\testdata\\evaluate\\ConfusionMatrix\\output"
                   )

  /**
    * 返回指标数据
    *
    * @param label
    *              类标
    * @param trueNum
    *                正确数
    * @param falseNum
    *                 错误数
    * @param totalNum
    *                 总数
    * @param accuracy
    *                 正确率，这里的正确率是计算的所有数据正确率
    * @param precision
    *                  精准率
    * @param recall
    *               召回率
    * @param f1Score
    *                f1 指标
    */
  case class IndexDetail(label: Double, trueNum: Int, falseNum: Int, totalNum: Int, accuracy: Double,precision:Double, recall: Double, f1Score: Double)

  def main(args: Array[String]) {
    var params = Params()
    val parser = new OptionParser[Params]("ConfusionMatrix") {
      head("ConfusionMatrix","2.0")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("labelCol")
        .required()
        .text("the label column")
        .action((x, c) => c.copy(labelCol = x))
      opt[String]("predictCol")
        .required()
        .text("the predict column")
        .action((x, c) => c.copy(predictCol = x))
      opt[String]("output_pt")
        .required()
        .text("Output file path")
        .action((x, c) => c.copy(output_pt = x))
    }
    parser.parse(args, params) match {
      case Some(params) => run(params)
      case None => System.exit(-1)
    }

  }

  def run(p: Params): Unit = {

    val spark = SparkSession.builder()
      .appName(AppUtils.AppName("ConfusionMatrix"))
//      .master("local")
      .getOrCreate()

    val rawDF = DFUtils.loadcsv(spark, p.input_pt)

    rawDF.show()

    val lableAndPredicDF = rawDF.select(col(p.predictCol), col(p.labelCol))


    val dataList = new util.ArrayList[Row]()
    val mm = new ConfusionMatrixUtils(lableAndPredicDF.rdd.map(r => (r(0).toString.toDouble, r(1).toString.toDouble)))
    val labelClasses = mm.labels

    var anmatrix = mm.confusionMatrix

    anmatrix.rowIter.zipWithIndex.foreach { case (vector, idx) => {
      val ab = new ArrayBuffer[Any]()
      ab.append(labelClasses(idx))
      vector.toArray.map(row => {
        ab.append(row)
      })
      dataList.add(Row.fromSeq(ab.toSeq))
    }
    }
    //构造DF列标题信息

    val structFieldList = labelClasses.map(col => {
      StructField(col.toString, DoubleType, true)
    }).toList

    val struct = StructType(StructField("labels", DoubleType, true) :: structFieldList)
    //构造混淆矩阵
    val resultDF = spark.createDataFrame(dataList, struct)

    DFUtils.exportjson(resultDF,PathUtils.getEvaluationConfusionMatrixPath(p.output_pt))


    import spark.implicits._

    //构建每类指标
    val indexData = mm.indexDetails()

    val indexDF = spark.sparkContext.parallelize(indexData).map(row=>{
      IndexDetail(row(0).toString.toDouble,row(1).toString.toInt,row(2).toString.toInt,row(3).toString.toInt,row(4).toString.toDouble,row(5).toString.toDouble,row(6).toString.toDouble,row(7).toString.toDouble)
    }).toDF()

//    indexDF.show(false)



    DFUtils.exportjson(indexDF, PathUtils.getEvaluationIndexPath(p.output_pt))
    spark.stop()
  }


}


class ConfusionMatrixUtils(predictionAndLabels: RDD[(Double, Double)]) {

  def this(predictionAndLabels: DataFrame) = this(predictionAndLabels.rdd.map(r => (r.getDouble(0), r.getDouble(1))))


  //每类对应的样本数量
  private val labelCountByClass: Map[Double, Long] = predictionAndLabels.values.countByValue()

  //总数量
  private val labelCount: Long = labelCountByClass.values.sum
  //每类预测正确的数据
  val tpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (label, if (label == prediction) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  //每类预测错误的数量
  val fpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (prediction, if (prediction != label) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()


  //混淆矩阵
  private val confusions = predictionAndLabels
    .map { case (prediction, label) =>
      ((label, prediction), 1)
    }.reduceByKey(_ + _)
    .collectAsMap()


  def confusionMatrix: Matrix = {
    val n = labels.length
    val values = Array.ofDim[Double](n * n)
    var i = 0
    while (i < n) {
      var j = 0
      while (j < n) {
        values(i + j * n) = confusions.getOrElse((labels(i), labels(j)), 0).toDouble
        j += 1
      }
      i += 1
    }
    Matrices.dense(n, n, values)
  }


  //计算指标数据
  //分类、正确数、错误数、总计、准确率、精确率、召回率、F1指标
  def indexDetails() = {

    this.labels.map(label => {
      Row(label, tpByClass(label), fpByClass(label), labelCountByClass(label), accuracy, precision(label), recall(label), fMeasure(label))
    })
  }

  /**
    * Returns true positive rate for a given label (category)
    *
    * @param label the label.
    */
  def truePositiveRate(label: Double): Double = recall(label)

  /**
    * Returns false positive rate for a given label (category)
    *
    * @param label the label.
    */
  def falsePositiveRate(label: Double): Double = {
    val fp = fpByClass.getOrElse(label, 0)
    fp.toDouble / (labelCount - labelCountByClass(label))
  }

  /**
    * Returns precision for a given label (category)
    *
    * @param label the label.
    */
  def precision(label: Double): Double = {
    val tp = tpByClass(label)
    val fp = fpByClass.getOrElse(label, 0)
    if (tp + fp == 0) 0 else tp.toDouble / (tp + fp)
  }

  /**
    * Returns recall for a given label (category)
    *
    * @param label the label.
    */
  def recall(label: Double): Double = tpByClass(label).toDouble / labelCountByClass(label)

  /**
    * Returns f-measure for a given label (category)
    *
    * @param label the label.
    * @param beta  the beta parameter.
    */
  def fMeasure(label: Double, beta: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    val betaSqrd = beta * beta
    if (p + r == 0) 0 else (1 + betaSqrd) * p * r / (betaSqrd * p + r)
  }

  /**
    * Returns f1-measure for a given label (category)
    *
    * @param label the label.
    */
  def fMeasure(label: Double): Double = fMeasure(label, 1.0)

  /**
    * Returns precision
    */
  @deprecated("Use accuracy.", "2.0.0")
  lazy val precision: Double = accuracy

  /**
    * Returns recall
    * (equals to precision for multiclass classifier
    * because sum of all false positives is equal to sum
    * of all false negatives)
    */
  @deprecated("Use accuracy.", "2.0.0")
  lazy val recall: Double = accuracy

  /**
    * Returns f-measure
    * (equals to precision and recall because precision equals recall)
    */
  @deprecated("Use accuracy.", "2.0.0")
  lazy val fMeasure: Double = accuracy

  /**
    * Returns accuracy
    * (equals to the total number of correctly classified instances
    * out of the total number of instances.)
    */
  lazy val accuracy: Double = tpByClass.values.sum.toDouble / labelCount

  /**
    * Returns weighted true positive rate
    * (equals to precision, recall and f-measure)
    */
  lazy val weightedTruePositiveRate: Double = weightedRecall

  /**
    * Returns weighted false positive rate
    */
  lazy val weightedFalsePositiveRate: Double = labelCountByClass.map { case (category, count) =>
    falsePositiveRate(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged recall
    * (equals to precision, recall and f-measure)
    */
  lazy val weightedRecall: Double = labelCountByClass.map { case (category, count) =>
    recall(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged precision
    */

  lazy val weightedPrecision: Double = labelCountByClass.map { case (category, count) =>
    precision(category) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged f-measure
    *
    * @param beta the beta parameter.
    */

  def weightedFMeasure(beta: Double): Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, beta) * count.toDouble / labelCount
  }.sum

  /**
    * Returns weighted averaged f1-measure
    */
  lazy val weightedFMeasure: Double = labelCountByClass.map { case (category, count) =>
    fMeasure(category, 1.0) * count.toDouble / labelCount
  }.sum

  /**
    * Returns the sequence of labels in ascending order
    */

  lazy val labels: Array[Double] = tpByClass.keys.toArray.sorted
}
