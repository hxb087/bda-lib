package com.golaxy.bda.statistics.analysis

import com.golaxy.bda.utils.DFUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
 * @author ：huxb
 * @date ：2020/10/12 11:08
 * @description：TODO
 * @modified By：
 * @version: $ 1.0
 *           todo from https://blog.csdn.net/Shea1992/article/details/
 *           82826105?utm_source=blogxgwz9&utm_medium=distribute.pc_relevant.none-task-blog-title-6&spm=1001.2101.3001.4242
 */


/** todo Params
--input_pt  D:\testdata\242\AssociationAnalysis
--output_pt D:\\testdata\\242\\AssociationAnalysis\\output
--input_col items
--limit  0.7
*/


object AprioriAnalysis {

//  Logger.getLogger("org").setLevel(Level.WARN)
//  Logger.getLogger("aka").setLevel(Level.WARN)

  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    input_col: String = "",
                    limit: Double = 0.7
                   )

  /**
   * 挖掘频繁项集
   */

  def main(args: Array[String]): Unit = {
    val params: Params = new Params()
    val parser: OptionParser[Params] = new OptionParser[Params]("AprioriAnalysis") {
      head("AprioriAnalysis")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("input_col")
        .required()
        .text("Operate columns")
        .action((x, c) => c.copy(input_col = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
      opt[Double]("limit")
        .required()
        .text("the limit")
        .action((x, c) => c.copy(limit = x))
    }

    parser.parse(args, params).map { params =>
      run(params)
    } getOrElse (System.exit(0))

  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("apriori")
      .getOrCreate()

    val sc = spark.sparkContext
    val rawDF: DataFrame = DFUtils.loadcsv(spark, params.input_pt)
    //dataframe转Array
    val mydata: Array[Array[Int]] = rawDF.select(params.input_col).collect.flatMap(_.toSeq)
      .map(t => t.toString().split(" ").map(_.toInt)) //.distinct

    //转化为rdd
    val pamydata: RDD[Array[Int]] = sc.parallelize(mydata)
    //获取数据集中的每项数据
    val C1: Array[Set[Int]] = pamydata.flatMap(_.toSet).distinct().collect().map(Set(_))
    //对每条数据去重
    val D = mydata.map(_.toSet)
    //广播数据集
    val D_bc = sc.broadcast(D)
    //获取数据集的条数大小
    val length = mydata.length

    //设置最小支持度
    var limit = params.limit
    //计算大于最小支持度的数据集（单个数据）
    var suppdata: Array[Any] = sc.parallelize(C1).map(f1(_, D_bc.value, length, limit)).filter(_.!=(())).collect()

    var L = Array[Array[Set[Int]]]()
    val L1 = suppdata.map(_ match {
      case a: Tuple2[_, _] => a._1 match {
        case b: Set[_] => b.asInstanceOf[Set[Int]]
      }
    })
    L = L :+ L1
    var k = 2

    while (L(k - 2).length > 0) {
      var CK = Array[Set[Int]]()
      for ((var1, index) <- L(k - 2).zipWithIndex; var2 <- L(k - 2).drop(index + 1) if var1.take(k - 2).equals(var2.take(k - 2))) {
        CK = CK :+ (var1 | var2)
      }

      val suppdata_temp = sc.parallelize(CK).map(f1(_, D_bc.value, length, limit)).filter(_.!=(())).collect()
      suppdata = suppdata :+ suppdata_temp
      L = L :+ suppdata_temp.map(_ match { case a: Tuple2[_, _] => a._1 match {
        case b: Set[_] => b.asInstanceOf[Set[Int]]
      }
      })
      k += 1
    }
    L = L.filter(_.nonEmpty)
//    L.foreach(_.foreach(println))

    val Ltodf: Array[String] = L.flatMap(_.map(_.toSeq.mkString(",")))

    val data = sc.makeRDD(Ltodf).map {Row(_)}

    val listcol = new ArrayBuffer[String]()
    listcol.append("data")

    val structFieldList1 = listcol.map(col => {
      StructField(col, StringType, true)
    }).toList
    val struct = StructType(structFieldList1)
    val data_df = spark.sqlContext.createDataFrame(data, struct)
    data_df.show(10)

    DFUtils.exportcsv(data_df, params.output_pt)
    spark.stop()
  }

  //计算每单个数据的支持度大于最小支持度的相关集合
  def f1(a: Set[Int], B: Array[Set[Int]], length: Int, limit: Double) = {
    //只查找每条包含了该数字的的数据集
    if (B.filter(b => a.subsetOf(b)).size / length.toDouble >= limit)
      (a, B.filter(b => a.subsetOf(b)).size / length.toDouble)
  }

}