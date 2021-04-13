package bda.spark.runnable.preprocess

import java.util.UUID

import bda.common.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object DataTransformRunner extends Logging {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("DataTransform") {
      head("DataTransform: Data transformation option in ETL...")
      opt[String]("script_fp")
        .required()
        .text("SQL script file path")
        .action((x, c) => c.copy(script_fp = x))
      opt[String]("input_kv")
        .required()
        .text("input key-value pairs in the form of \"name:file-path;name:file-path;...;name:file-path\"")
        .action((x, c) => c.copy(input_kv = x))
      opt[String]("output_kv")
        .required()
        .text("output key-value pairs in the form of \"name:file-path;name:file-path;...;name:file-path\"")
        .action((x, c) => c.copy(output_kv = x))
      note(
        """
          |For example, the following command runs this app on your cluster
          |
          | bin/spark-submit --class bda.runnable.preprocess.DataTransformRunner \
          |   out/artifacts/*/*.jar \
          |   --script_fp ... \
          |   --input_kv ... \
          |   --output_kv ...
        """.stripMargin
      )
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of DataTransform")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sql_context = new HiveContext(sc)

    val uuid = UUID.randomUUID().toString.split("-").mkString("")
    logInfo(s"db_name=$uuid")

    sql_context.sql(s"create database if not exists $uuid")

    try {
      // choose database
      sql_context.sql(s"use $uuid")

      // load data from disks
      params.input_kv.split(";").filter(!_.trim.isEmpty).foreach { kv =>
        val Array(name, fp) = kv.split(":")
        val df = sql_context.read.json(fp)
        df.registerTempTable(name)
      }

      // load sql script file
      val sqls = sc.textFile(params.script_fp).collect().mkString(" ")

      // run SQL command
      sqls.split(";").filter(!_.trim.isEmpty).foreach { sql =>
        logInfo(s"sql=($sql)")
        sql_context.sql(sql)
      }

      // save data sets on disks
      params.output_kv.split(";").filter(!_.trim.isEmpty).foreach { kv =>
        val Array(name, fp) = kv.split(":")
        val df = sql_context.sql(s"select * from $name")
        df.write.json(fp)
      }
    } finally {
      sql_context.sql(s"drop database if exists $uuid cascade")
    }
  }

  /**
    * Case class of params
    *
    * @param script_fp SQL script file path
    * @param input_kv input key-value pairs in the form of "name:file-path;name:file-path;...;name:file-path"
    * @param output_kv output key-value pairs in the form of "name:file-path;name:file-path;...;name:file-path"
    */
  case class Params(script_fp: String = "",
                    input_kv: String = "",
                    output_kv: String = "")
}