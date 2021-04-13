package com.golaxy.bda.utils

import org.apache.spark.sql.types.{StructField, DataType, StructType}

/**
  * Created by Administrator on 2020/3/27.
  */
object BDASchemaUtils {


  def checkColumnType(schema: StructType,
                      colName: String,
                      dataType: DataType): Unit = {
    checkColumnType(schema, colName, dataType, "")
  }

  //判断列类型
  def checkColumnType(
                       schema: StructType,
                       colName: String,
                       dataType: DataType,
                       msg: String = ""): Unit = {
    val actualDataType = schema(colName).dataType
    val message = if (msg != null && msg.trim.length > 0) " " + msg else ""
    require(actualDataType.equals(dataType),
      s"Column $colName must be of type $dataType but was actually $actualDataType.$message")
  }

  /**
    * 判断schema中是否存在colName列
    *
    * @param schema
    * @param colName
    * @return
    */
  def columnIsExists(schema: StructType, colName: String): Boolean = schema.fieldNames.contains(colName)


}
