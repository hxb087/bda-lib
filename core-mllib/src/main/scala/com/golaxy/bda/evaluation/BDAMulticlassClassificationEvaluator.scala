package org.apache.spark.ml.evaluation

/**
 * @author ：ljf
 * @date ：2020/11/18 15:13
 * @description：bda BDAMulticlassClassificationEvaluator
 * @modified By：
 * @version: $ 1.0
 */

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
 * :: Experimental ::
 * Evaluator for multiclass classification, which expects two input columns: prediction and label.
 */
@Since("1.5.0")
@Experimental
class BDAMulticlassClassificationEvaluator @Since("1.5.0")(@Since("1.5.0") override val uid: String)
  extends BDAEvaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  val allowedParams = Array("f1", "weightedPrecision", "weightedRecall", "accuracy")

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("mcEval"))

  /**
   * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
   * `"weightedRecall"`, `"accuracy"`)
   *
   * @group param
   */
  @Since("1.5.0")
  val metricName: Param[String] = {
    val allowedParam = ParamValidators.inArray(allowedParams)
    new Param(this, "metricName", "metric name in evaluation " +
      "(f1|weightedPrecision|weightedRecall|accuracy)", allowedParam)
  }

  /** @group getParam */


  /** @group setParam */
  @Since("1.5.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "f1")

  @Since("2.0.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val metric = $(metricName) match {
      case "f1" => metrics.weightedFMeasure
      case "weightedPrecision" => metrics.weightedPrecision
      case "weightedRecall" => metrics.weightedRecall
      case "accuracy" => metrics.accuracy
    }
    metric
  }

  @Since("1.5.0")
  override def isLargerBetter: Boolean = true

  @Since("1.5.0")
  override def copy(extra: ParamMap): BDAMulticlassClassificationEvaluator = defaultCopy(extra)

  override def evaluateAll(dataset: Dataset[_]):  Map[String, Double] = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(predictionCol), DoubleType)
    SchemaUtils.checkNumericType(schema, $(labelCol))

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)

    val tuples: Array[(String, Double)] = Array(
      "f1" -> metrics.weightedFMeasure,
      "weightedPrecision" -> metrics.weightedPrecision,
      "weightedRecall" -> metrics.weightedRecall,
      "accuracy" -> metrics.accuracy)
    tuples.toMap
  }

  @Since("1.5.0")
  override def getMetricName(): String = $(metricName)
}

@Since("1.6.0")
object BDAMulticlassClassificationEvaluator
  extends DefaultParamsReadable[BDAMulticlassClassificationEvaluator] {

  @Since("1.6.0")
  override def load(path: String): BDAMulticlassClassificationEvaluator = super.load(path)
}

