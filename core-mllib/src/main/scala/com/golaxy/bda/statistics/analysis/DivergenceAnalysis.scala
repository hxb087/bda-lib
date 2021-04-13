package com.golaxy.bda.statistics.analysis

import com.golaxy.bda.statistics.analysis.common.AnalysisUtils.metricsMap
import com.golaxy.bda.utils.{AppUtils, DFUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.stat.descriptive.numerical.DivergenceSummarizer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser

import scala.collection.mutable

/**
 * @author ：ljf
 * @date ：2020/9/22 16:42
 * @description：divergence analysis
 *                         has "minimum value,maximum value,range,var,std,coefficient of variation"
 * @modified By：
 * @version: $ 1.0
 */
object DivergenceAnalysis {

  /** example
   * --metrics mean,sum,variance,std,numNonZeros,max,min,normL2,normL1,range,groupInterval,variationCoefficient
   * --inputDataPath data/mllib/iris.csv --outputPath out/credit/divergenceAnalysis --inputCols V0,V1,V2,V3
   */
  case class Params(inputDataPath: String = "",
                    inputCols: String = "",
                    metrics: String = "",
                    outputPath: String = "")

  def main(args: Array[String]): Unit = {
    val default_params: Params = Params()
    val parser = new OptionParser[Params]("DivergenceAnalysis") {
      head("DivergenceAnalysis", "2.0")
      opt[String]("inputDataPath")
        .required()
        .text("Test Input file path")
        .action((x, c) => c.copy(inputDataPath = x))
      opt[String]("inputCols")
        .required()
        .text("Input columns name")
        .action((x, c) => c.copy(inputCols = x))
      opt[String]("metrics")
        .required()
        .text("the metrics name")
        .action((x, c) => c.copy(metrics = x))
      opt[String]("outputPath")
        .required()
        .text("the path of output data")
        .action((x, c) => c.copy(outputPath = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(AppUtils.AppName("DivergenceAnalysis"))
      .getOrCreate()

    import DivergenceSummarizer._
    import spark.implicits._

    val rawDF: DataFrame = DFUtils.loadcsv(spark, p.inputDataPath)
    val inputCols: Array[String] = p.inputCols.split(",")

    //assembler
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
    val featureDF: DataFrame = assembler.transform(rawDF)

    val selectMetrics: Array[String] = p.metrics.split(",")
    val aliasName = "summary"
    val aliasMetricsName = selectMetrics.map(x => aliasName.concat("." + x))

    //metrics
    DivergenceSummarizer.setGroupsNumber(10)
    val metricsDF: DataFrame = featureDF.select(metrics(selectMetrics: _*)
      .summary($"features").as(aliasName))
      .select(aliasMetricsName.head, aliasMetricsName.tail: _*)
    val (minValues, maxValues, groupInterval) = metricsDF.select("min", "max", "groupInterval")
      .as[(Vector, Vector, Vector)].first()


    //density map
    val featureSize = inputCols.length
    val segmentAndCount = new Array[(Array[Double], Array[Int])](featureSize)
    for (i <- 0 until featureSize) {
      var groupSegment = new Array[Double](groupsNumber + 1)
      val segmentCount: Array[Int] = new Array[Int](groupsNumber)
      //      groupSegment = groupSegment.zipWithIndex.map(item => minValues(i) + item._2 * groupInterval(i))
      updateIntervalArray(minValues(i), groupInterval(i), groupSegment)
      //解决区间累加精度丢失问题，最大值更新为max value
      groupSegment(groupsNumber) = maxValues(i)
      segmentAndCount.update(i, (groupSegment, segmentCount))
    }

    //broadcast
    val segmentAndCountBC = spark.sparkContext.broadcast(segmentAndCount)

    featureDF.select("features").rdd.map(row => {
      updateCountOfSegment(row, segmentAndCountBC)
    }).collect()

    val colAndDensityMap = new Array[(String, mutable.Map[String, Int])](featureSize)
    for (i <- 0 until featureSize) {
      val colName = inputCols(i)
      val (segments, counts) = segmentAndCount(i)
      //生成x轴坐标
      var groupNames = segments.map(elem => elem.formatted("%.2f"))
      groupNames = groupNames.slice(0, groupNames.length - 1).zipWithIndex.map {
        item => item._1.concat(s"-${groupNames(item._2 + 1)}")
      }
      val densityMap = new mutable.HashMap[String, Int]()
      groupNames.zip(counts).map(item => densityMap.put(item._1, item._2)) // 填充数据
      colAndDensityMap.update(i, (colName, densityMap))
    }
    val densityMapDF = colAndDensityMap.toSeq.toDF("colName", "densityMap")


    val replacedMetrics = selectMetrics.map(metric => metricsMap.getOrElse(metric, metric))
    import com.golaxy.bda.statistics.analysis.common.Implicit._
    //use implicit class transpose DataFrame and add one column
    val resultDF = metricsDF.transpose(inputCols: _*)
      .addColumn(replacedMetrics, "metrics", StringType, false)

    DFUtils.exportjson(densityMapDF, p.outputPath + "_evaluation/densityMap")
    DFUtils.exportcsv(resultDF, p.outputPath)
    spark.stop()
  }

  /**
   * interval search and update
   *
   * @param row
   * @param segmentAndCountBC
   */
  def updateCountOfSegment(row: Row, segmentAndCountBC: Broadcast[Array[(Array[Double], Array[Int])]]): Unit = {
    val vector: Vector = row.getAs[Vector](0)

    for (i <- 0 until vector.size) {
      val value: Double = vector(i)
      val groupSegment: Array[Double] = segmentAndCountBC.value(i)._1
      val indexOption = findPosition(value, groupSegment)
      val index: Int = indexOption.getOrElse {
        throw new IllegalArgumentException(s"value $value cannot be found correct interval location." +
          s" in sort array: ${groupSegment.mkString}")
      }

      //update segment count
      val segmentCount: Array[Int] = segmentAndCountBC.value(i)._2
      segmentCount.update(index, segmentCount(index) + 1)
    }
  }

  /**
   * 输入待查找数据和有序数组，返回查找数据在有序数组中的位置，二分查找
   * 如果待查找数据不在有序数组中返回-1
   */
  def findPosition(elem: Double, sortArray: Array[Double]): Option[Int] = {
    var low = 0
    var high = sortArray.length
    var rightPosition = -1
    var pivot = (high + low) >> 1
    while (low < high && pivot >= 0 && pivot < sortArray.length - 1) {
      //查找到对应的区间范围
      if (sortArray(pivot) <= elem && elem <= sortArray(pivot + 1)) {
        rightPosition = pivot
        return Option(rightPosition)
        //上半区间
      } else if (elem >= sortArray(pivot + 1)) {
        low = pivot
        //下半区间
      } else {
        high = pivot
      }
      pivot = (high + low) >> 1
    }

    None
  }


  /**
   * 更新全为0的区间数组
   *
   * @param minValue  ：最小值
   * @param interval  ：区间值
   * @param zeroArray : 全零数组
   */
  def updateIntervalArray(minValue: Double, interval: Double, zeroArray: Array[Double]): Unit = {
    var current = minValue
    for (i <- zeroArray.indices) {
      zeroArray.update(i, current)
      current += interval;
    }
  }
}


