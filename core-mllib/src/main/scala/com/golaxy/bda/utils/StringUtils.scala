package com.golaxy.bda.utils

/**
 * 字符串处理工具类
 */
object StringUtils {

  case class JDBCInfo( hostname:String="",port:String,dbName:String="")

  def ParseJDBCURL(url:String): JDBCInfo ={
    val arr =url.split("/")
    new JDBCInfo(arr(2).split(":")(0),arr(2).split(":")(1),arr(3))
  }
}
