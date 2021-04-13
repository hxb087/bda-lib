package com.golaxy.bda.statistics.ParameterEstimation

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
  * 方差估计
  * input_pt:数据输入
  * appname:应用名称
  */


object VarianceEstimation {
  /** command line parameters */
  case class Params(input_pt: String = "",
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

      val input_pt1 = p.input_pt
      val appname = p.appname

      val spark = SparkSession.builder()
        .appName(appname)
        .getOrCreate()
      val sc = spark.sparkContext
      val df1 = DFUtils.loadcsv(spark,input_pt1)
      val col1 = df1.columns

      val dataRdd1 = df1.rdd.map{
        row =>
          Vectors.dense(row.toSeq.toArray.map(m=>m.toString.toDouble))
      }
      val summary = Statistics.colStats(dataRdd1)
      val colrdd = sc.parallelize(Seq(col1.mkString(",")))
      val outrdd = colrdd.union(sc.parallelize(Seq(summary.variance.toArray.mkString(","))))
      outrdd.collect().foreach(println)
      spark.stop()
    }
  }
}
