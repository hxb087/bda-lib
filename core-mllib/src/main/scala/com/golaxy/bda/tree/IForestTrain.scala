package com.golaxy.bda.tree

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tree.{IForest, IForestModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}
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


/*--input_pt
data/mllib/win.csv
--input_Feature
fixed_acidity,volatile_acidity,citric_acid,residual_sugar
--label
quality
--model_pt
out/IForestModel
--NumTrees
100
--Contamination
0.35
--Bootstrap
false
--MaxDepth
100
--Seed
123456
 */


object IForestTrain {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    input_Feature: String = "",
                    label: String = "",
                    model_pt: String = "",
                    NumTrees:Int = 100,
                    Contamination:Double = 0.35,
                    Bootstrap:Boolean = false,
                    MaxDepth:Int = 100,
                    Seed:Int = 123456
                   )

  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("IForest") {
      head("IForest")
      opt[String]("input_Feature")
        .required()
        .text("input_Feature")
        .action((x, c) => c.copy(input_Feature = x))
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("label")
        .required()
        .text("label")
        .action((x, c) => c.copy(label = x))
      opt[String]("model_pt")
        .required()
        .text("Output model file path")
        .action((x, c) => c.copy(model_pt = x))
      opt[Int]("NumTrees")
        .required()
        .text("NumTrees")
        .action((x, c) => c.copy(NumTrees = x))
      opt[Double]("Contamination")
        .required()
        .text("Contamination")
        .action((x, c) => c.copy(Contamination = x))
      opt[Boolean]("Bootstrap")
        .required()
        .text("Bootstrap")
        .action((x, c) => c.copy(Bootstrap = x))
      opt[Int]("MaxDepth")
        .required()
        .text("MaxDepth")
        .action((x, c) => c.copy(MaxDepth = x))
      opt[Int]("Seed")
        .required()
        .text("Seed")
        .action((x, c) => c.copy(Seed = x))

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

    val dataset = DFUtils.loadcsv(spark, p.input_pt)
    val indexer = new StringIndexer()
      .setInputCol(p.label)
      .setOutputCol("label_use")

    val features = p.input_Feature.split(",")
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")

    val iForest = new IForest()
      .setNumTrees(p.NumTrees)
      .setContamination(p.Contamination)
      .setBootstrap(p.Bootstrap)
      .setMaxDepth(p.MaxDepth)
      .setSeed(p.Seed)
      .setLabelCol("label_use")

    val pipeline = new Pipeline().setStages(Array(indexer, assembler, iForest))
    val model = pipeline.fit(dataset)
    model.save(p.model_pt)


  }
}
