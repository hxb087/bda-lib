package com.golaxy.bda.statistics.ParameterEstimation

import com.golaxy.bda.utils.DFUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
  * 均值差估计
  * input_pt:数据输入
  * input_pf:数据输入
  * appname:应用名称
  */

object MeanDifferenceEstimation {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    input_pf: String = "",
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
          Vectors.dense(row.toSeq.toArray.map(m=>m.toString.toDouble))
      }
      val dataRdd2 = df2.rdd.map{
        row =>
          Vectors.dense(row.toSeq.toArray.map(m=>m.toString.toDouble))
      }
      val mean1 = Statistics.colStats(dataRdd1).mean
      val mean2 = Statistics.colStats(dataRdd2).mean
      val diffBuffer = ArrayBuffer[Double]()
      for(i <- Range(0,mean1.size)){
        var difference = mean1(i) - mean2(i)
        diffBuffer.append(difference)
      }


      val colrdd = sc.parallelize(Seq(col1.mkString(",")))
      val outrdd = colrdd.union(sc.parallelize(Seq(diffBuffer.mkString(","))))
      outrdd.collect().foreach(println)
      spark.stop()
    }
  }
}
