package com.golaxy.bda.regression

import com.golaxy.bda.utils.{AppUtils, DFUtils}
import org.apache.spark.ml.regression.GBTRegressionModel
import org.apache.spark.ml.{PipeUtils, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

import scala.collection.mutable

/**
 * @author ：ljf
 * @date ：2020/7/21 16:03
 * @description：基于DataFrame的梯度提升回归树预测算子
 * @modified By：
 * @version: $ 1.0
 */

object GBTRegressorPredict {
  /**
   * command line example
   * --input_model data/output/GBDRegressor --input_pt data/mllib/sample_linear_regression_data.csv
   * --predictCol table --output_pt data/predict/GBDRegressor
   */
  case class Params(input_pt: String = "",
                    input_model: String = "",
                    predictCol: String = "",
                    output_pt: String = "")

  def main(args: Array[String]): Unit = {
    val default_params: Params = Params()
    val parser = new OptionParser[Params]("GBTRegressor predict") {
      head("GBTRegressorPredict", "2.0")
      opt[String]("input_pt")
        .required()
        .text("Test Input file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("predictCol")
        .required()
        .text("the predict column name")
        .action((x, c) => c.copy(predictCol = x))
      opt[String]("input_model")
        .required()
        .text("Input model file path")
        .action((x, c) => c.copy(input_model = x))
      opt[String]("output_pt")
        .required()
        .text("Output file path")
        .action((x, c) => c.copy(output_pt = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    //创建sparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName(AppUtils.AppName("GBTRegressorPredict"))
      .getOrCreate()

    //加载模型和预测数据
    val preDF: DataFrame = DFUtils.loadcsv(spark, p.input_pt)
    val model: PipelineModel = PipelineModel.load(p.input_model)
    model.stages(1).asInstanceOf[GBTRegressionModel].setPredictionCol(p.predictCol)

    val result: DataFrame = model.transform(preDF)
    val selectDF: DataFrame = result.drop("features")

    //导出预测结果
    val pipeStages = PipeUtils.loadBeforeStages(spark, p.input_pt)

    PipeUtils.exportTotalStage(p.output_pt, spark, pipeStages, model)
    DFUtils.exportcsv(selectDF, p.output_pt)
    spark.stop()
  }
}
