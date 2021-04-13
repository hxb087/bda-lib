package com.golaxy.bda.statistics.analysis

/**
 * @author ：huxb
 * @date ：2020/10/10 17:59
 * @description：TODO
 * @modified By：
 * @version: $ 1.0
 */

import com.golaxy.bda.utils.DFUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
--input_pt
D:\testdata\242\AssociationAnalysis
--output_pt
D:\\testdata\\242\\AssociationAnalysis\\output_model
--input_col
items
--MinSupport
0.2
--MinConfidence
0.2
--PredictionCol
prediction
*/


object FPGrowthAnalysisTrain {
//  Logger.getLogger("org").setLevel(Level.WARN)
//  Logger.getLogger("aka").setLevel(Level.WARN)

  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    input_col: String = "",
                    MinSupport:Double = 0.2,
                    MinConfidence:Double =0.6,
                    PredictionCol:String = ""
                   )

  /**
   * 挖掘频繁项集
   */

  def main(args: Array[String]): Unit = {
    val params: Params = new Params()
    val parser: OptionParser[Params] = new OptionParser[Params]("PFGrowthAnalysisTrain") {
      head("PFGrowthAnalysisTrain")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("input_col")
        .required()
        .text("Operate columns")
        .action((x, c) => c.copy(input_col = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
      opt[Double]("MinSupport")
        .required()
        .text("the MinSupport")
        .action((x, c) => c.copy(MinSupport = x))
      opt[Double]("MinConfidence")
        .required()
        .text("the MinConfidence")
        .action((x, c) => c.copy(MinConfidence = x))
      opt[String]("PredictionCol")
        .required()
        .text("the PredictionCol")
        .action((x, c) => c.copy(PredictionCol = x))
    }

    parser.parse(args, params).map { params =>
      run(params)
    } getOrElse (System.exit(0))

  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val rawDF: DataFrame = DFUtils.loadcsv(spark, params.input_pt)

    val dataset = rawDF.select(params.input_col).map(t => t.toString().replace("[", "")
      .replace("]", "").split(" ")).toDF("items")
    dataset.show()

    val arrayBuffer = ArrayBuffer[PipelineStage]()
    val fpgrowth = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(params.MinSupport)
      .setMinConfidence(params.MinConfidence)
      .setPredictionCol(params.PredictionCol)

    arrayBuffer.append(fpgrowth)
    val pipeline = new Pipeline().setStages(arrayBuffer.toArray)
    val model= pipeline.fit(dataset)

    // Display frequent itemsets.
    model.stages(0).asInstanceOf[FPGrowthModel].freqItemsets.show()

    // Display generated association rules.
    model.stages(0).asInstanceOf[FPGrowthModel].associationRules.show()

    // consequents as prediction
    model.transform(dataset).show()

    model.save(params.output_pt)
    spark.stop()
  }
}
