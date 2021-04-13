package com.golaxy.bda.regression

import com.golaxy.bda.utils.{AppUtils, DFUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author ：ljf
 * @date ：2020/7/21 15:45
 * @description：基于DataFrame的梯度提升回归树训练算子
 * @modified By：
 * @version: $ 1.0
 */

object GBTRegressorTrain {
  /** example
   * --input_pt data/mllib/sample_linear_regression_data.csv --features V0,V1,V2,V3,V4,V5,V6,V7,V8,V9
   * --label label --impurity gini --maxDepth 5 --maxIter 20 --output_pt data/output/GBDRegressor
   */
  case class Params(input_pt: String = "",
                    features: String = "",
                    label: String = "",
                    impurity: String = "gini",
                    maxDepth: Int = 5,
                    maxIter:Int = 20,
                    output_pt: String = "")

  def main(args: Array[String]): Unit = {
    val default_params = Params()
    val parser = new OptionParser[Params]("GBTRegressor train") {
      head("GBTRegressor train", "2.0")
      opt[String]("input_pt")
        .required()
        .text("input data path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("features")
        .required()
        .text("select columns of input data")
        .action((x, c) => c.copy(features = x))
      opt[String]("label")
        .required()
        .text("label column of training data")
        .action((x, c) => c.copy(label = x))
      opt[String]("output_pt")
        .required()
        .text("output path of model checkpoint")
        .action((x, c) => c.copy(output_pt = x))
      opt[String]("impurity")
        .required()
        .text(" Criterion used for information gain calculation (case-insensitive) (default = gini)")
        .action((x, c) => c.copy(impurity = x))
      opt[Int]("maxDepth")
        .required()
        .text("max depth of tree")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxIter")
        .required()
        .text("max Iteration of training model")
        .action((x, c) => c.copy(maxIter = x))
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p:Params): Unit ={
    val spark = SparkSession
      .builder
      .master("local")
      .appName(AppUtils.AppName("GBTRegressorTrain"))
      .getOrCreate()

    // Load and parse the data file, converting it to a DataFrame.
    val rawDF = DFUtils.loadcsv(spark,p.input_pt)

    val inputCols = p.features.split(",")
    val assembler: VectorAssembler = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")


    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol(p.label)
      .setFeaturesCol("features")
      .setMaxIter(p.maxIter)
      .setMaxDepth(p.maxDepth)
      .setImpurity(p.impurity)


    // Chain indexer and GBT in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(assembler, gbt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(rawDF)

    model.save(p.output_pt)
    spark.stop()
  }
}
