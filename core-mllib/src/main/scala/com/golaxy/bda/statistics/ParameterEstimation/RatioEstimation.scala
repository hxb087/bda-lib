package com.golaxy.bda.statistics.ParameterEstimation

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  * 比率估计
  * input_pt:数据输入
  * molecular_str:分子
  * select_col:选择列
  * appname:应用名称
  */

object RatioEstimation {
  /** command line parameters */
  case class Params(input_pt: String = "",
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
      val appname = p.appname

      val spark = SparkSession.builder()
        .appName(appname)
        .getOrCreate()
      val sc = spark.sparkContext
      val df1 = DFUtils.loadcsv(spark,input_pt1)
      val col1 = df1.columns
      val dataRdd1 = df1.select(selectCol).rdd.map{
        row =>
          row.toSeq.toArray.map(m=>m.toString)
      }
      dataRdd1.cache()
      val sum = dataRdd1.count()
      val moleCount = dataRdd1.filter(f=>molecular.equals(f(0))).count()
      val rate = moleCount / sum.toDouble
      val colrdd = sc.parallelize(Seq(selectCol))
      val outrdd = colrdd.union(sc.parallelize(Seq(rate.toString)))
      println(rate)
      spark.stop()
    }
  }
}
