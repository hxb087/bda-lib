package com.golaxy.bda.evaluate

import com.golaxy.bda.utils.{DFUtils, AppUtils}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, DataType}
import scopt.OptionParser

/**
  * Created by Administrator on 2020/3/27.
  * 回归结果评估
  * 需要输入
  */
object RegressionEvaluate {

  case class Params(
                     input_pt: String = "D:\\testdata\\wine\\lr\\predict",
                     lable_col: String = "quality",
                     predict_col: String = "predictcol",
                     output_pt: String = "D:\\testdata\\wine\\lr\\eva"
                   )

  def main(args: Array[String]) {

    val default_param = Params()

    val parser = new OptionParser[Params]("RegressionEvaluate") {
            head("RegressionEvaluate")
            opt[String]("input_pt")
              .required()
              .text("Input document file path")
              .action((x, c) => c.copy(input_pt = x))
            opt[String]("lable_col")
              .required()
              .text("label")
              .action((x, c) => c.copy(lable_col = x))
            opt[String]("predict_col")
              .required()
              .text("predict_col")
              .action((x, c) => c.copy(predict_col = x))
            opt[String]("output_pt")
              .required()
              .text("Output document file path")
              .action((x, c) => c.copy(output_pt = x))
    }

    parser.parse(args, default_param) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }

  }

  def run(params: Params): Unit = {
    val spark = SparkSession.builder()
      .appName(AppUtils.AppName("RegressionEvaluate"))
//      .master("local")
      .getOrCreate()

    val predDF = DFUtils.loadcsv(spark, params.input_pt)

    val schema = predDF.schema

    //判断字段类型
    //    SchemaUtils.checkColumnType(schema, params.lable_col, DoubleType)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(params.lable_col)
      .setPredictionCol(params.predict_col)


    val arrMetricNames = Array("rmse", "mse", "mae")


    val seqV = arrMetricNames.map(name => {
      val v = evaluator.setMetricName(name).evaluate(predDF)
      (name, v)
    })

    val detailDF = spark.createDataFrame(seqV).toDF("Metrics","Values")

//    detailDF.show(false)
    DFUtils.exportcsv(detailDF,params.output_pt)

    spark.stop()
  }

}
