package com.golaxy.bda.evaluate


import java.util

import breeze.numerics.ceil
import com.golaxy.bda.utils.{AppUtils, DFUtils, PathUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.dmg.pmml.ROC
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * https://blog.csdn.net/snaillup/article/details/75348830
  * https://blog.csdn.net/snaillup/article/details/75348830
  */
object BinaryClassificationRunner {

  /** command line parameters */
  case class Params(input_pt: String = "",
                    label: String = "",
                    scorecol: String = "",
                    bins: Int = 4,
                    output_pt: String = ""
                   )

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)


    val default_params = Params()
    val parser = new OptionParser[Params]("BinaryClassificationRunner") {
      head("BinaryClassificationRunner")
            opt[String]("input_pt")
              .required()
              .text("Input document file path")
              .action((x, c) => c.copy(input_pt = x))
            opt[String]("label")
              .required()
              .text("label")
              .action((x, c) => c.copy(label = x))
            opt[String]("output_pt")
              .required()
              .text("Output document file path")
              .action((x, c) => c.copy(output_pt = x))
            opt[String]("scorecol")
              .required()
              .text("the predict score column")
              .action((x, c) => c.copy(scorecol = x))
            opt[Int]("bins")
              .required()
              .text("the bins number")
              .action((x, c) => c.copy(bins = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

  }

  def run(p: Params): Unit = {
    val input_pt = p.input_pt
    val output_pt = p.output_pt
    val label = p.label
    val scorecol = p.scorecol
    val spark = SparkSession.builder()
      .appName(AppUtils.AppName("BinaryClassificationRunner"))
//      .master("local[4]")
      .getOrCreate()
    val df = DFUtils.loadcsv(spark, input_pt)


    //    df.printSchema()
    val scoreAndLabels = df.select(p.scorecol, p.label).rdd.map { m => {
//      val trueScore = if (m(0) == 1) m(0).asInstanceOf[Double] else 1 - m(0).asInstanceOf[Double]
//      val trueScore = if (m(0) == 1) m(0).toString.toDouble.asInstanceOf[Double] else 1 - m(0).toString.toDouble.asInstanceOf[Double]
//      (trueScore, m(1).toString.toDouble)
      (m(0).toString.toDouble,m(1).toString.toDouble)
    }
    }.repartition(1)


    val baseIndexData: BaseIndexData = new BaseIndexData(scoreAndLabels, p.bins)
    val indexdata = baseIndexData.getIndexData
    val indexDF = spark.createDataFrame(indexdata).toDF("index", "value")

    DFUtils.exportcsv(indexDF, p.output_pt)

    DFUtils.exportjson(indexDF, PathUtils.EvaluationBinaryIndexDataPath(p.output_pt))
    val rocCurve = spark.createDataFrame(baseIndexData.roc).toDF("FPR", "TPR")

    DFUtils.exportjson(rocCurve, PathUtils.EvaluationBinaryROCCurve(p.output_pt))


    val prCurve = spark.createDataFrame(baseIndexData.PRC).toDF("Presion", "Recall")
//    prCurve.show(false)

    DFUtils.exportjson(prCurve, PathUtils.EvaluationBinaryPRCurve(p.output_pt))
    import spark.implicits._
    //等频数据
    //分数，总数，正例数，负例数，FPR，Precision，Recall，F1
    val frequent: EvaluateEqualFrequent = new EvaluateEqualFrequent(scoreAndLabels, p.bins)

//    frequent.FrequentData.foreach(println)
    val frequentDF = frequent.FrequentData.map(d => FrequentData(d._1, d._2, d._3, d._4, d._5, d._6, d._7, d._8)).toDF()


//    frequentDF.show(false)
    DFUtils.exportjson(frequentDF, PathUtils.EvaluationBinaryFrequentDataPath(p.output_pt))


    //等宽数据
    //    val width: EvaluateEqualWidth = new EvaluateEqualWidth(df.select("scoresCol", "label"), 10)

    //    val predcitAndLabels = df.select("predict", "label").rdd.map { m => (m(0).toString.toDouble, m(1).toString.toDouble) }
    //    //    scoreAndLabels.collect().foreach(println)
    //
    //    val accuracy = Accuracy(predcitAndLabels)
    //
    //    println("===============accuracy=====================")
    //    println("accuracy=" + accuracy)


    spark.stop()
  }



}

case class FrequentData(score: Double, total: Long, Postive: Long, Nagtive: Long, FPR: Double, Precision: Double, Recall: Double, F1: Double)

class BaseIndexData(scoreAndLabels: RDD[(Double, Double)], bins: Int) extends Serializable {


  val bcMetrics: BinaryClassificationMetrics = new BinaryClassificationMetrics(scoreAndLabels, bins)

  //计算Accuracy、Precision、Recall、f1_score

  val records = scoreAndLabels.count()

  println("records=" + records)

  val tPrecision = bcMetrics.precisionByThreshold()
  val precision = tPrecision.map(_._2).max()
  println("precision=" + precision)

  val tRecall = bcMetrics.recallByThreshold()
  val recallv = tRecall.map(_._2).max()
  println("recall=" + recallv)
  val tFMeasure = bcMetrics.fMeasureByThreshold()
  val f1score = tFMeasure.map(_._2).max()

  println("f1score=" + f1score)


  val precisionByThreshold: RDD[(Double, Double)] = bcMetrics.precisionByThreshold()

  println("==========precisionByThreshold=============")
  //  precisionByThreshold.foreach { case (t, p) => println(s"Threshold: $t, Precision: $p") }

  println("==========recallByThreshold=============")
  val recall = bcMetrics.recallByThreshold()
  //  recall.foreach { case (t, r) => println(s"Threshold:$t,recall:$r") }

  //     Precision-Recall Curve
  println("==========Precision-Recall Curve=============")
  val PRC = bcMetrics.pr

  //  PRC.foreach { case (p, r) => println(s"Precision:$p,recall:$r") }

  //     F-measure
  println("==========F-measure-Recall Curve=============")
  val f1Score = bcMetrics.fMeasureByThreshold

  //  f1Score.foreach { case (t, f) =>
  //    println(s"Threshold: $t, F-score: $f, Beta = 1")
  //  }


  //     AUPRC
  println("==========AUPRC=============")
  val auPRC = bcMetrics.areaUnderPR

  println("Area under precision-recall curve = " + auPRC)


  // Compute thresholds used in ROC and PR curves

  val thresholds = bcMetrics.thresholds()

  //     ROC Curve
  val roc = bcMetrics.roc()

  //     AUROC
  val auROC = bcMetrics.areaUnderROC

  println("Area under ROC = " + auROC)


  //计算acc

  val predictAndLabel = scoreAndLabels.map(t => {
    val predict = if (t._1 > 0.5) 1.0 else 0.0
    (predict, t._2)
  })


  val acc = Accuracy(predictAndLabel)

  /**
    * 准确率计算
    *
    * @param preditionAndLabel
    * @return
    */
  def Accuracy(preditionAndLabel: RDD[(Double, Double)]): Double = {
    val tpNum = preditionAndLabel.map { case (prediction, label) =>
      if (prediction == label) 1 else 0
    }.reduce(_ + _)

    val records = preditionAndLabel.count()

    //    println("records=" + records)
    tpNum.toDouble / records
  }
  val getIndexData: List[(String, Double)] = {

    val list = List(("num", records.toDouble),("Accuracy",acc), ("precision", precision), ("recall", recallv), ("f1score", f1score))
    list
  }

  /**
    * ROC曲线数据
    */
  val getROC: Array[(Double, Double)] = {
    bcMetrics.roc().collect()
  }
}


/**
  * 根据scoreAndLabels数据计算等宽数据
  *
  * @param scoreAndLabels
  */
class EvaluateEqualWidth(scoreAndLabels: DataFrame, bins: Int) extends Serializable {

  //  def this(scoreAndLabels: DataFrame, bins: Int) = {
  //    this(scoreAndLabels.rdd.map(r => (r.getDouble(0), r.getDouble(1))), bins)
  //  }

  private val minScore = scoreAndLabels.rdd.map(row => row(0).asInstanceOf[Double]).min()
  private val maxScore = scoreAndLabels.rdd.map(row => row(0).asInstanceOf[Double]).max()
  private val distance = (maxScore.toDouble - minScore.toDouble) / bins


  val scoreArr = (minScore to maxScore by distance).toArray
  //  scoreArr.foreach(println)

  val bucketizer = new Bucketizer()
    .setInputCol("scoresCol")
    .setOutputCol("scoreBin")
    .setSplits(scoreArr.toArray)

  val buckedData = bucketizer.transform(scoreAndLabels)


  //  buckedData.printSchema()
  //  buckedData.groupBy("scoreBin").count().show(false)
  val binedCount = buckedData.rdd.map {
    case Row(score: Double, label: Int, binIndex: Double) => (scoreArr(binIndex.asInstanceOf[Int]), score)
  }.combineByKey(
    createCombiner = (score: Double) => new BinaryLabelCounter(0L, 0L) += score,
    mergeValue = (c: BinaryLabelCounter, score: Double) => c += score,
    mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
  ).sortByKey()


}

/**
  * 计算等频数据
  *
  * @param scoreAndLabels
  * @param bins
  */
class EvaluateEqualFrequent(scoreAndLabels: RDD[(Double, Double)], bins: Int) extends Serializable {
  println(scoreAndLabels.getNumPartitions)
  val counts = scoreAndLabels.combineByKey(
    createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label,
    mergeValue = (c: BinaryLabelCounter, label: Double) => c += label,
    mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
  ).sortByKey(ascending = false)

  /*counts.foreach(println)  //看看*/

  val binsCount = counts.count() / bins + 1
  val binnedCountsByScore = scoreAndLabels.sortByKey(false).mapPartitions(_.grouped(binsCount.toInt).map { pairs =>
    // The score of the combined point will be just the first one's score
    val firstScore = pairs.last._1
    // The point will contain all counts in this chunk
    val agg = new BinaryLabelCounter()
    //          println("pairs.length=" + pairs.length)
    pairs.foreach(pair => agg += pair._2)
    (firstScore, agg)
  })

  val aggByScore = binnedCountsByScore.values.mapPartitions { iter =>
    val agg = new BinaryLabelCounter()
    iter.foreach(agg += _)
    Iterator(agg)
  }.collect()


  val partitionwiseCumulativeCountsByScore =
    aggByScore.scanLeft(new BinaryLabelCounter())((agg, c) => agg.clone() += c)


  val cumulativeCountsByScore = binnedCountsByScore.mapPartitionsWithIndex(
    (index: Int, iter: Iterator[(Double, BinaryLabelCounter)]) => {
      val cumCount = partitionwiseCumulativeCountsByScore(index)
      iter.map { case (score, c) =>
        cumCount += c
        (score, cumCount.clone())
      }
    }, preservesPartitioning = true)

  val confusionsByScore = cumulativeCountsByScore.map { case (score, cumCount) =>
    (score, BinaryConfusionMatrixImpl(cumCount, partitionwiseCumulativeCountsByScore.last).asInstanceOf[BinaryConfusionMatrix])
  }


  val joinrdd = binnedCountsByScore.join(confusionsByScore)

  val FrequentData = joinrdd.sortByKey(false).map { case (score: Double, (bc: BinaryLabelCounter, bcm: BinaryConfusionMatrix)) => {
    /**
      * 分数，总数，正例数，负例数，FPR，Precision，Recall，F1
      */
    val precision = bcm.numTruePositives.toDouble / (bcm.numTruePositives + bcm.numFalsePositives)
    val fpr = bcm.numFalsePositives.toDouble / (bcm.numFalsePositives + bcm.numTrueNegatives)
    val recall = bcm.numTruePositives.toDouble / (bcm.numTruePositives + bcm.numFalseNegatives)
    val f1 = 2 * recall * precision / (recall + precision)
    (score, bc.total, bc.numPositives, bc.numNegatives, fpr, precision, recall, f1)
  }
  }


  val binnedCounts =
  // Only down-sample if bins is > 0
    if (bins == 0) {
      // Use original directly
      counts
    } else {
      val countsSize = counts.count()
      // Group the iterator into chunks of about countsSize / numBins points,
      // so that the resulting number of bins is about numBins

      var grouping = countsSize / bins + 1
      if (grouping < 2) {
        counts
      } else {
        if (grouping >= Int.MaxValue) {
          grouping = Int.MaxValue
        }
        counts.mapPartitions(_.grouped(grouping.toInt).map { pairs =>

          // The score of the combined point will be just the first one's score
          val firstScore = pairs.head._1
          // The point will contain all counts in this chunk
          val agg = new BinaryLabelCounter()
          //          println("pairs.length=" + pairs.length)
          pairs.foreach(pair => agg += pair._2)
          (firstScore, agg)
        })
      }
    }


  val agg = binnedCounts.values.mapPartitions { iter =>
    val agg = new BinaryLabelCounter()
    iter.foreach(agg += _)
    Iterator(agg)
  }.collect()
  val partitionwiseCumulativeCounts =
    agg.scanLeft(new BinaryLabelCounter())((agg, c) => agg.clone() += c)
  val totalCount = partitionwiseCumulativeCounts.last

  val cumulativeCounts = binnedCounts.mapPartitionsWithIndex(
    (index: Int, iter: Iterator[(Double, BinaryLabelCounter)]) => {
      val cumCount = partitionwiseCumulativeCounts(index)
      iter.map { case (score, c) =>
        cumCount += c
        (score, cumCount.clone())
      }
    }, preservesPartitioning = true)
  cumulativeCounts.persist()
  val confusions = cumulativeCounts.map { case (score, cumCount) =>
    (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
  }


}

class BinaryLabelCounter(
                          var numPositives: Long = 0L,
                          var numNegatives: Long = 0L) extends Serializable {
  /** Processes a label. */
  def +=(label: Double): BinaryLabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    //    println("score="+label)
    if (label > 0.5) numPositives += 1L else numNegatives += 1L
    this
  }

  /** Merges another counter. */
  def +=(other: BinaryLabelCounter): BinaryLabelCounter = {
    numPositives += other.numPositives
    numNegatives += other.numNegatives
    this
  }

  override def clone: BinaryLabelCounter = {
    new BinaryLabelCounter(numPositives, numNegatives)
  }

  def total = numNegatives + numPositives

  override def toString: String = s"{numPos: $numPositives, numNeg: $numNegatives,numTotal:$total}"
}


