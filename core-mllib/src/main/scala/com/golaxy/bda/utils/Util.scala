package com.golaxy.bda.utils

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


object Util {

  case class JDBCInfo(hostname: String = "", port: String, dbName: String = "")

  def ParseJDBCURL(url: String): JDBCInfo = {
    val arr = url.split("/")
    JDBCInfo(arr(2).split(":")(0), arr(2).split(":")(1), arr(3))
  }

  /**
   * import the file and transform the data into dataframe according to the file format.
   *
   * @param sqlContext
   * @param format   origin data format.
   * @param input_pt input file path.
   * @return the dataframe.
   */
  def transToDataframe(sqlContext: SQLContext, input_pt: String, format: String): DataFrame = {
    val df: DataFrame = format match {
      case "parquet" => sqlContext.read.parquet(input_pt)
      case "json" => sqlContext.read.json(input_pt)
      case "csv" => sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", ",")
        .load(input_pt)
      case "tsv" => sqlContext.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "\t")
        .load(input_pt)
      case _ => throw new IllegalArgumentException(s"Bad format ${format}")
    }
    df
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
   * import the file and transform the data into dataframe according to the file format.
   *
   * @param spark
   * @param format   origin data format.
   * @param input_pt input file path.
   * @return the dataframe.
   */
  def transToDataframe(spark: SparkSession, input_pt: String, format: String): DataFrame = {
    val df: DataFrame = format match {
      case "parquet" => spark.read.parquet(input_pt)
      case "json" => spark.read.json(input_pt)
      case "csv" => spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", true)
        .option("delimiter", ",")
        .load(input_pt)
      case "tsv" => spark.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "\t")
        .load(input_pt)
      case _ => throw new IllegalArgumentException(s"Bad format ${format}")
    }
    df
  }

  def conn_postgres_createDB(ip: String, db: String, user: String, passwd: String) = {
    val conn_str = "jdbc:postgresql://" + ip + "postgres"
    Class.forName("org.postgresql.Driver").newInstance
    val conn = DriverManager.getConnection(conn_str, user, passwd)
    // Configure to be Read Only
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    // Execute Query
    val rs = statement.executeQuery("SELECT *  FROM pg_catalog.pg_database u where trim(u.datname)='" + db + "'")
    if (rs.next().==(false)) {
      statement.execute("create database " + db)
    }
  }
}
