package bda.spark.labelpoint

import bda.common.Logging
import bda.common.obj.Rate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * This part transform the json data into labelpoint.
  */
object JsonToRate extends Logging{
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new scopt.OptionParser[Params]("JsonToRate") {
      head("JsonToRate", "1.0")
      opt[String]("input_pt").required()
        .text("input_pt is the path stores the json file.")
        .action { (x, c) => c.copy(input_pt = x) }
      opt[String]("output_pt").required()
        .text(("output_pt is the path stores the labelpoint file"))
        .action { (x, c) => c.copy(output_pt = x) }
      opt[String]("user_col").required()
        .text("user_col is the column which is treated as the user")
        .action { (x, c) => c.copy(user_col = x) }
      opt[String]("item_col").required()
        .text("item_col is the column which is treated as the item")
        .action { (x, c) => c.copy(item_col = x) }
      opt[String]("rate_col")
        .text("rate_col is the column which is treated as the rate")
        .action { (x, c) => c.copy(rate_col = x) }
      help("help").text("prints this usage text")
    }
    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  private def run(para: Params): Unit = {
    val conf = new SparkConf()
      .setAppName("JsonToRate")
      .set("spark.hadoop.validateOutputSpecs", "false")
    //.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tableDF = sqlContext.read.json(para.input_pt)
    val cnt = tableDF.count()
    logInfo(s"there are ${cnt} records in this file.")
    transformToRate(tableDF, para.user_col, para.item_col, para.rate_col, para.output_pt)
  }

  /**
    * * Transform a dataframe into a rate file.
    * Mainly used for matrix factorization or graph algorithm.
    *
    * @param tableDF
    * @param user_col user column
    * @param item_col item column
    * @param rate_col rate column, if the column is not identified, the rate is 1.0
    * @param output_pt the output path for rate file.
    */
  private def transformToRate(tableDF: DataFrame, user_col: String, item_col: String, rate_col: String, output_pt: String): Unit = {

    val rds = tableDF.rdd.map {
      ln =>
        val user = ln.getAs(user_col).toString.toInt
        val item = ln.getAs(item_col).toString.toInt
        val rate = if (rate_col.equals("")) 1.0 else ln.getAs(rate_col).toString.toDouble
        new Rate(user, item, rate)
    }
    rds.map {
      ln =>
        ln.toString
    }.saveAsTextFile(output_pt)
  }

  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    user_col: String = "",
                    item_col: String = "",
                    rate_col: String = "")

}

