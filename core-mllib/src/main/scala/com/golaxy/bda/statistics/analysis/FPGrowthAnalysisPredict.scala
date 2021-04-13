package com.golaxy.bda.statistics.analysis

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.{PipeUtils, PipelineModel}
import org.apache.spark.ml.fpm.{FPGrowthModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser
import scala.collection.mutable

/**
 * @author ：huxb
 * @date ：2020/10/14 13:34
 * @description：TODO
 * @modified By：
 * @version: $ 1.0
 */

/** todo params
 * --input_pt D:\testdata\242\AssociationAnalysis
--input_model D:\testdata\242\AssociationAnalysis\output_model
--output_pt D:\\testdata\\242\\AssociationAnalysis\\output_PFGrowth*/

object FPGrowthAnalysisPredict {
  case class Params(input_pt: String = "",
                    input_model: String = "",
                    output_pt: String = ""
                   )
  /**
   * 挖掘频繁项集
   */

  def main(args: Array[String]): Unit = {
    val params: Params = new Params()
    val parser: OptionParser[Params] = new OptionParser[Params]("PFGrowthAnalysisPredict") {
      head("PFGrowthAnalysisPredict")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("input_model")
        .required()
        .text("input_model file path")
        .action((x, c) => c.copy(input_model = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
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
    val model: PipelineModel = PipelineModel.load(params.input_model)
    val input_col = model.stages(0).asInstanceOf[FPGrowthModel].getItemsCol
    val dataset = rawDF.select(input_col).map(t => t.toString().replace("[", "")
      .replace("]", "").split(" ")).toDF("items")

    val result = model.transform(dataset)
    //    result.show()

    val toArr: (Any) => String = _.asInstanceOf[mutable.WrappedArray[String]].mkString(" ")
    import org.apache.spark.sql.functions._

    val toArrUdf = udf(toArr) //spark.udf.register("toArr", toArr )
    val resultStr = result.withColumn("items", toArrUdf(col("items")))
      .withColumn("prediction", toArrUdf(col("prediction")))
    resultStr.show(false)

    //导出预测结果
    val pipeStages = PipeUtils.loadBeforeStages(spark, params.input_pt)
    PipeUtils.exportTotalStage(params.output_pt, spark, pipeStages, model)
    DFUtils.exportcsv(resultStr, params.output_pt)

    spark.stop()
  }
}