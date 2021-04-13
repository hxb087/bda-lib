package com.golaxy.bda.statistics.pivot

import com.golaxy.bda.utils.{DFUtils, AppUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

/**
  * Created by Administrator on 2020/4/1.
  * 数据透视图转换，行转列过程
  */
object DataConvertPivot {

  /**
    *
    * @param input_pt
    * 输入数据
    * @param groupcol
    * 分组列
    * @param pivotcol
    * 转换列，需要转换成列的数据
    * @param computedcol
    * 计算列，对列进行sum操作
    * @param output_pt
    * 输出路径
    */
  case class Params(
                     input_pt: String = "D:\\testdata\\pivot\\pivot.csv",
                     groupcol: String = "year",
                     pivotcol: String = "month",
                     computedcol: String = "count",
                     output_pt: String = "D:\\testdata\\pivot\\output"
                   )


  def main(args: Array[String]) {
    val params = new Params()
    val parser: OptionParser[Params] = new OptionParser[Params]("DataConvertPivot") {
      head("DataConvertPivot", "1.0")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("groupcol")
        .required()
        .text("group column")
        .action((x, c) => c.copy(groupcol = x))
      opt[String]("pivotcol")
        .required()
        .text("pivot column")
        .action((x, c) => c.copy(pivotcol = x))
      opt[String]("computedcol")
        .required()
        .text("computedcol column")
        .action((x, c) => c.copy(computedcol = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
    }

    parser.parse(args, params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession.builder()
      .appName(AppUtils.AppName("DataConvertPivot"))
//            .master("local")
      .getOrCreate()

    val rawDF: DataFrame = DFUtils.loadcsv(spark, params.input_pt)

    val pivotDF = rawDF.groupBy(params.groupcol)
      .pivot(params.pivotcol)
      .sum(params.computedcol)
      .na.fill(0)

        pivotDF.show(10)

    DFUtils.exportcsv(pivotDF, params.output_pt)

    spark.stop()
  }

}
