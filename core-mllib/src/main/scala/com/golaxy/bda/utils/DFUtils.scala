package com.golaxy.bda.utils

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DFUtils {

  /**
   * 预测算子添加列信息，自定义函数
   */
  val scoreDetails: DenseVector => String = (arg: DenseVector) => {
    arg.toString()
  }

  val addScoreDetailsCol: UserDefinedFunction = udf(scoreDetails)
  val positiveScore: DenseVector => Double = (arg: DenseVector) => {
    arg.values.max
  }

  val addPositiveScoreCol: UserDefinedFunction = udf(positiveScore)

  /**
   * 新列代替旧列的数据,并和旧列的数据顺序保持一致
   *
   * @param olddf 旧数据
   * @param newdf 新数据
   * @param str   新列名称比旧列名称多的字符
   * @return
   */
  def colTransfrom(olddf: DataFrame, newdf: DataFrame, str: String): DataFrame = {
    var df = newdf
    val oldcols = olddf.columns
    val newcols = df.columns
    val diffcols = newcols.diff(oldcols)
    //判断是否有新列,没有返回旧列
    if (diffcols == null || diffcols.length == 0) {
      return olddf
    }
    //进行转换
    for (diffcol <- diffcols) {
      var newindex = diffcol.indexOf(str)
      var oldname = diffcol.substring(0, newindex)
      var newdfdrop = df.drop(oldname)
      df = newdfdrop.withColumnRenamed(diffcol, oldname)
    }
    df.select(oldcols.head, oldcols.tail: _*)
  }

  /**
   * 导入csv的数据转换成dataframe
   *
   * @param spark
   * @param path
   * @return
   */
  def loadcsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .load(path)
  }

  /**
   * 导入csv的数据转换成dataframe
   *
   * @param spark
   * @param path
   * @return
   */
  def loadcsv(spark: SparkSession, path: String, delimiter: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(path)
  }

  /**
   * 将dataframe的数据导出csv
   *
   * @param df
   * @param path
   */
  def exportcsv(df: DataFrame, path: String): Unit = {
    val delimiter = ","
    exportcsv(df, path, delimiter, false)
  }


  /**
   * 将dataframe的数据导出csv
   *
   * @param df
   * @param path
   * @param delimiter
   */
  def exportcsv(df: DataFrame, path: String, delimiter: String): Unit = {
    this.exportcsv(df, path, delimiter, false)
  }


  /**
   * 导出csv格式文件
   *
   * @param df
   * @param path
   * @param delimiter
   * @param execNullData 是否清空空值对应行数据
   */
  def exportcsv(df: DataFrame, path: String, delimiter: String, execNullData: Boolean): Unit = {

    val data = if (execNullData) df.na.drop() else df
    data.write.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("delimiter", delimiter) //默认以","分割m
      .save(path)
  }


  /**
   * 没有头文件的情况下  动态创建数据结构信息列名以 col_i   i是从0开始的数字
   *
   * @param spark       SparkSession
   * @param rdd         String类型的RDD
   * @param splitSymbol 每条信息的分割符号
   * @return DataFrame
   */
  def mkDataFrame(spark: SparkSession, rdd: RDD[String], splitSymbol: String) = {
    val colsLength = rdd.first.split(splitSymbol).length
    val colNames = new Array[String](colsLength)
    for (i <- 0 until colsLength) {
      colNames(i) = "_c" + i
    }
    // 将RDD动态转为DataFrame
    // 设置DataFrame的结构
    val schema = StructType(colNames.map(fieldName => StructField(fieldName, StringType)))
    // 对每一行的数据进行处理
    val rowRdd = rdd.map(_.split(",")).map(p => Row(p: _*))
    spark.createDataFrame(rowRdd, schema)
  }

  /**
   * Write a dataframe into a file according to its format.
   *
   * @param output_format output file format, like json, csv, tsv, parquet.
   * @param df            souce dataframe.
   * @param output_pt     output file path.
   */
  def outputToFile(output_pt: String, df: DataFrame, output_format: String): Unit = {
    output_format match {
      case "tsv" =>
        df.write.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", "\t")
          .save(output_pt)
      case "csv" =>
        df.write.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("delimiter", ",")
          .save(output_pt)
      case "parquet" => df.write.parquet(output_pt)
      case "json" => df.write.json(output_pt)
      case _ => throw new IllegalArgumentException(s"Bad format ${output_format}")
    }
  }

  /**
   * 计算总得维度
   *
   * @param rdd
   * @return
   */
  def comNumFeatures(rdd: RDD[(Int, Double, Array[Int], Array[Double])]): Int = {
    rdd.map { case (rowid, label, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }


  /**
   * 数据进行转化
   *
   * @param s
   * @param split
   * @return
   */
  def transDate(s: String, split: String): (Double, Array[Int], Array[Double]) = {
    val items = s.split(split)
    val label = items.head.toDouble
    val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
      val indexAndValue = item.split(':')
      val index = indexAndValue(0).toDouble.toInt // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
      (index, value)
    }.unzip
    // check if indices are one-based and in ascending order
    var previous = -1
    var i = 0
    val indicesLength = indices.length
    //判断是否是升序
    //    while (i < indicesLength) {
    //      val current = indices(i)
    //      require(current > previous, s"indices should be one-based and in ascending order;"
    //        +
    //        s""" found current=$current, previous=$previous; line="$s"""")
    //      previous = current
    //      i += 1
    //    }
    (label, indices, values)
  }


  /**
   *
   * @param s
   * @param split
   * @param has_rowid
   * @return
   */
  def transDate(s: String, split: String, has_rowid: Boolean): (Int, Double, Array[Int], Array[Double]) = {

    if (has_rowid) {
      val rowid = s.split(split).head.toInt
      val items = s.split(split).tail.mkString(" ")
      transDate(items, split) match {
        case (label, indices, values) => (rowid, label, indices, values)
      }
    } else {
      transDate(s, split) match {
        case (label, indices, values) => (0, label, indices, values)
      }
    }

  }


  /**
   *
   * @param df
   * @param path
   * 将Dataframe到处Json格式文件
   */
  def exportjson(df: DataFrame, path: String): Unit = df.repartition(1).write.json(path)

}

