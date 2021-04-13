package com.golaxy.bda.feature


import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.feature.{PCA, PCAModel, VectorAssembler}
import org.apache.spark.ml.{PipeUtils, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
 * 主成分析算法
 * input_pt:输入数据
 * inputModle_pt:输入模型
 * pcaNum:主成维度
 * output_pt:输出数据
 * appname:应用名称
 */
object FeaturePCA_Transform {

  /** example
   * --input_pt data/mllib/sample_pca_data.csv   --inputModel_pt out/output/pca-model --output_pt out/predict/pca_predict
   *
   * @Params
   */
  /** command line parameters */
  case class Params(input_pt: String = "",
                    inputModel_pt: String = "",
                    output_pt: String = ""
                   )

  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("FeaturePCA_Transform") {
      head("FeaturePCA_Transform")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("inputModel_pt")
        .required()
        .text("Input model file path")
        .action((x, c) => c.copy(inputModel_pt = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(p: Params): Unit = {

    val spark = SparkSession.builder()
      .appName("FeaturePCA_Transform")
      .master("local")
      .getOrCreate()

    //读入文件
    val df = DFUtils.loadcsv(spark, p.input_pt)

    val model = PipelineModel.load(p.inputModel_pt)
    val pcaData = model.transform(df)


    val toData1 = pcaData.select("VectorAssemblerFeaturesPCA").rdd.map {
      line =>
        line(0).asInstanceOf[DenseVector].toArray
    }
    val toData2 = toData1.map {
      Row.fromSeq(_)
    }

    val listCol1 = new ArrayBuffer[String]()
    for (i <- 1 to model.stages(1).asInstanceOf[PCAModel].getK) {
      listCol1.append("col" + i)
    }

    val structFieldList = listCol1.map(col => {
      StructField(col, DoubleType, true)
    }).toList
    val struct = StructType(structFieldList)
    val resultDF = spark.sqlContext.createDataFrame(toData2, struct)

    val pipeStages = PipeUtils.loadBeforeStages(spark, p.input_pt)
    PipeUtils.exportTotalStage(p.output_pt, spark, pipeStages, model)
    DFUtils.exportcsv(resultDF, p.output_pt)

  }
}
