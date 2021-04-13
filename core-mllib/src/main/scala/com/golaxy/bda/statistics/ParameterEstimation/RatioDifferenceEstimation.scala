package com.golaxy.bda.statistics.ParameterEstimation

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  * 比率差估计
  * input_pt:数据输入
  * input_pf:数据输入
  * molecular_str:分子
  * select_col:选择列
  * appname:应用名称
  */


object RatioDifferenceEstimation {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    input_pf: String = "",
                    molecular_str: String = "",
                    select_col: String = "",
                    appname: String = ""
                   )
  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("statistics") {
      head("StringIndex")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("input_pf")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pf = x))
      opt[String]("molecular_str")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(molecular_str = x))
      opt[String]("select_col")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(select_col = x))
      opt[String]("appname")
        .required()
        .text("appname")
        .action((x, c) => c.copy(appname = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
    def run(p: Params): Unit = {

      val molecular = p.molecular_str
      val selectCol = p.select_col
      val input_pt1 = p.input_pt
      val input_pt2 = p.input_pf
      val appname = p.appname
      val spark = SparkSession.builder()
        .appName(appname)
        .getOrCreate()
      val sc = spark.sparkContext
      val df1 = DFUtils.loadcsv(spark,input_pt1)
      val df2 = DFUtils.loadcsv(spark,input_pt2)
      val col1 = df1.columns

      val dataRdd1 = df1.rdd.map{
        row =>
          row.toSeq.toArray.map(m=>m.toString)
      }
      val dataRdd2 = df2.rdd.map{
        row =>
          row.toSeq.toArray.map(m=>m.toString)
      }
      dataRdd1.cache()
      dataRdd2.cache()
      val sum1 = dataRdd1.count()
      val sum2 = dataRdd2.count()
      val moleCount1 = dataRdd1.filter(f=>molecular.equals(f(0))).count()
      val moleCount2 = dataRdd2.filter(f=>molecular.equals(f(0))).count()
      val rate1 = moleCount1 / sum1.toDouble
      val rate2 = moleCount2 / sum2.toDouble
      val rateDifference = rate1 - rate2
      val colrdd = sc.parallelize(Seq(selectCol))
      val outrdd = colrdd.union(sc.parallelize(Seq(rateDifference.toString)))
      println(rateDifference)
      spark.stop()
    }
  }
}
