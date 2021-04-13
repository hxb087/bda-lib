package com.golaxy.bda.tree

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.{PipeUtils, Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tree.IForest
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author ：ljf
 * @date ：2020/10/19 11:03
 * @description：
 * @modified By：
 * @version: $ 1.0
 */

/**
 * @author ：huxb
 * @date ：2020/11/23 11:03
 * @description：
 * @modified By：
 * @version: $ 1.0
 */

object IForestPredict {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    model_pt: String = "",
                    output_pt: String = ""
                   )

  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("IForest") {
      head("IForest")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("model_pt")
        .required()
        .text("Output model file path")
        .action((x, c) => c.copy(model_pt = x))
      opt[String]("output_pt")
        .required()
        .text("output_pt")
        .action((x, c) => c.copy(output_pt = x))

    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(p: Params): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("iforest")
      .getOrCreate()


    val df = DFUtils.loadcsv(spark, p.input_pt)
    val model = PipelineModel.load(p.model_pt)
    val resultDF = model.transform(df).drop("features")
    resultDF.show(5)

    val pipeStages = PipeUtils.loadBeforeStages(spark, p.input_pt)
    PipeUtils.exportTotalStage(p.output_pt, spark, pipeStages, model)
    DFUtils.exportcsv(resultDF, p.output_pt)

  }
}
