package com.golaxy.bda.feature

/**
 * Created by huxb on 2020/07/07.
 * 修改结果的csv
 * 暂时只做到这两个分析
 */

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.ml.feature.{PCA, PCAModel, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
 * 主成分析算法
 * input_pt:输入数据
 * output_col:选择特征结果
 * outmodel_pt:输出模型
 * output_explained: 结果评测
 * pcaNum:主成维度
 */


object FeaturePCA {

  /** example
   * --input_pt data/mllib/sample_pca_data.csv --input_Feature volatile_acidity,pH,sulphates,alcohol
   * --model_pt out/output/pca-model --output_explained out/output/pca-explained --pcaNum 3
   *
   * @param
   */

  /** command line parameters */
  case class Params(input_pt: String = "",
                    input_Feature: String = "",
                    model_pt: String = "",
                    output_explained: String = "",
                    pcaNum: Int = 3
                   )

  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("PCA") {
      head("PCA")
      opt[String]("input_Feature")
        .required()
        .text("input_Feature")
        .action((x, c) => c.copy(input_Feature = x))
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("model_pt")
        .required()
        .text("Output model file path")
        .action((x, c) => c.copy(model_pt = x))
      opt[String]("output_explained")
        .required()
        .text("Output explain file path")
        .action((x, c) => c.copy(output_explained = x))
      opt[Int]("pcaNum")
        .required()
        .text("pcaNum")
        .action((x, c) => c.copy(pcaNum = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(p: Params): Unit = {

    val spark = SparkSession.builder()
      .appName("FeaturePCA")
      .master("local")
      .getOrCreate()
    //读入文件
    var df = DFUtils.loadcsv(spark, p.input_pt)

    //读取要做PCA的特征
    val features = p.input_Feature.split(",")
    //建立PipelineStage可变数组，用于存放过程
    val arrayBuffer = ArrayBuffer[PipelineStage]()

    //特征合并
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol("VectorAssemblerFeatures")
    arrayBuffer.append(assembler)

    //主成成分分析
    val pca = new PCA().setK(p.pcaNum).setInputCol("VectorAssemblerFeatures").setOutputCol("VectorAssemblerFeaturesPCA")

    //    特征合并
    arrayBuffer.append(pca)

    val pipeline = new Pipeline().setStages(arrayBuffer.toArray)
    val model = pipeline.fit(df)

    //pca分析数据
    val explainedArray = model.stages(1).asInstanceOf[PCAModel].explainedVariance.values
    val explainedBuff = new ArrayBuffer[Row]()
    var sum = 0.0

    //   构建rdd转dataframe的格式
    val sc = spark.sparkContext
    val structFields = Array(StructField("rows", IntegerType, true), StructField("explained", DoubleType, true), StructField("sum_explained", DoubleType, true))
    val structType = StructType(structFields)

    for (i <- Range(0, explainedArray.size)) {
      sum += explainedArray(i)
      explainedBuff.append(Row(i, explainedArray(i), sum))
    }
    val explainedRdd = sc.parallelize(explainedBuff).repartition(1)
    val explainedDF = spark.createDataFrame(explainedRdd, structType)

    DFUtils.exportcsv(explainedDF, p.output_explained)
    model.save(p.model_pt)
  }
}
