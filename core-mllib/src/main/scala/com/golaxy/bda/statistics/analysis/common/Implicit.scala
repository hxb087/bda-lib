package com.golaxy.bda.statistics.analysis.common

import java.util

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{DataType, DoubleType, Metadata, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col

/**
 * @author ：ljf
 * @date ：2020/10/13 13:51
 * @description：隐式类增强
 * @modified By：
 * @version: $ 1.0
 */
object Implicit {

  implicit class TransformTolls(df: DataFrame) extends Serializable {
    val sc = df.sparkSession.sparkContext
    val spark = df.sparkSession

    import spark.implicits._

    /**
     * row transpose,element type is SeqLike
     *
     * @param row
     */
    def shrinkRow(row: Row): util.List[Row] = {
      val result = new util.ArrayList[Row](row.size)
      for (i <- 0 until row.size) {
        result.add(i, Row.fromSeq(row.getAs[Vector](i).toArray))
      }
      result
    }

    def transpose(newCols: String*): DataFrame = {
      val fields = newCols
        .map(fieldName => StructField(fieldName, DoubleType, nullable = true))
      val schema = StructType(fields)

      // Convert records of the RDD (people) to Rows
      val rows: util.List[Row] = shrinkRow(df.first())

      // Apply the schema to the RDD
      val resultDF = df.sparkSession.createDataFrame(rows, schema)

      resultDF
    }

    def addColumn(elements: Seq[Any], colName: String, dataType: DataType, nullable: Boolean = true): DataFrame = {
      val rdd = sc.parallelize(elements, df.rdd.getNumPartitions)
      val newRdd = df.rdd.zip(rdd).map(r => Row.fromSeq(Seq(r._2) ++ r._1.toSeq))

      val schemaArray: Array[StructField] = df.schema.toArray
      val fields: List[StructField] = StructField(colName, dataType, nullable, Metadata.empty) :: Nil ++ schemaArray
      // create a new data frame from the rdd_new with modified schema
      spark.createDataFrame(newRdd, StructType(fields))
    }

    def addRow(elements: Seq[Any]): DataFrame = {
      val rdd = sc.parallelize(elements, df.rdd.getNumPartitions).map(item => Row(item))
      spark.createDataFrame(rdd.union(df.rdd), df.schema)
    }

    def addZScore(colName: String, otherCols: String*): DataFrame = {
      var mean = df.select(colName).as[Double].rdd.mean()
      var std = df.select(colName).as[Double].rdd.stdev()

      var resultDF = df.withColumn(colName + "z-score", col(colName) - mean / std)
      for (elem <- otherCols) {
        mean = df.select(elem).as[Double].rdd.mean()
        std = df.select(elem).as[Double].rdd.stdev()
        resultDF = resultDF.withColumn(elem + "z-score", col(elem) - mean / std)
      }
      resultDF
    }
  }

}
