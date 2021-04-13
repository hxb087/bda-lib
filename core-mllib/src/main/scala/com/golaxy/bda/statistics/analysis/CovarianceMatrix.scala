package com.golaxy.bda.statistics.analysis

import java.util

import com.golaxy.bda.utils.{DFUtils, AppUtils}
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{types, Row, SparkSession}
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer


/**
  * Created by Lijg on 2020/3/11.
  * 协方差矩阵算子
  */
object CovarianceMatrix {

  /**
    *
    * @param input_pt   输入数据路径
    * @param output_pt  输出数据路径
    * @param input_cols 输入列信息，多列之间","分割，数值型列
    */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    input_cols: String = ""
                   )

  //  case class Params(input_pt: String = "D:\\testdata\\credit\\rawdata\\creditcard.csv",
  //                  output_pt: String = "D:\\testdata\\credit\\optdata",
  //                  input_cols: String = "V1,V2,V4,V5"
  //                 )


  def main(args: Array[String]) {
    val params = new Params()
    val optParser = new OptionParser[Params]("CovarianceMatrix") {
      head("CovarianceMatrix")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("input_cols")
        .required()
        .text("Operate columns")
        .action((x, c) => c.copy(input_cols = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
    }
    optParser.parse(args, params).map { params =>
      run(params)
    } getOrElse {
      System.exit(0)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName(AppUtils.AppName("CovarianceMatrix"))
      .getOrCreate()

    import spark.implicits._

    val inputDF = DFUtils.loadcsv(spark, params.input_pt)

    inputDF.show()

    //TODO 校验字段类型，只有数值型字段才符合条件


    inputDF.createOrReplaceTempView("CovarianceMatrix")

    //获得操作列数据
    val optDF = spark.sql("select " + params.input_cols + " from CovarianceMatrix")

    //生成RDDdata
    val rddData = optDF.rdd.map { row =>
      Vectors.dense(row.toSeq.toArray.map { x => x.asInstanceOf[Double] })
    }
    //生成RowMatrix
    val mat: RowMatrix = new RowMatrix(rddData)

    //计算协方差
    val covariance: Matrix = mat.computeCovariance()


    //构造DF数据列
    val colList = params.input_cols.split(",").toList
    val dataList = new util.ArrayList[Row]()
    val rows = covariance.rowIter.zipWithIndex.foreach { case (vector, idx) => {
      //      println(s"idx:$idx,colList[$idx]:${ colList(idx)}")
      val ab = new ArrayBuffer[Any]()
      ab.append(colList(idx))
      vector.toArray.map(row => {
        ab.append(row)
      })
      dataList.add(Row.fromSeq(ab.toSeq))
    }
    }

    //构造DF列标题信息
    val fieldArr = new Array[StructField](colList.length + 1)

    val structFieldList = colList.map(col => {
      StructField(col, DoubleType, true)
    }).toList

    val struct = StructType(StructField("dim", StringType, true) :: structFieldList)


    val resultDF = spark.sqlContext.createDataFrame(dataList, struct)

    resultDF.show()

    DFUtils.exportcsv(resultDF, params.output_pt)


    spark.stop()

  }

}
