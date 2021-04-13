package com.golaxy.bda.evaluate

import com.golaxy.bda.evaluate.BinaryClassifPr.run
import com.golaxy.bda.utils.DFUtils
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object BinaryClassifROC {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    label: String = "",
                    predict:String = "",
                    output_pt: String = ""
                   )
  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("BinaryClassifROC") {
      head("BinaryClassifROC")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("label")
        .required()
        .text("label")
        .action((x, c) => c.copy(label = x))
      opt[String]("predict")
        .required()
        .text("predict")
        .action((x, c) => c.copy(predict = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }

  }
  def run(p: Params): Unit = {
    val input_pt = p.input_pt
    val output_pt = p.output_pt
    val label = p.label
    val predict = p.predict
    val spark = SparkSession.builder().appName("BinaryClassifROC").getOrCreate()
    val df = DFUtils.loadcsv(spark,input_pt,"\t").select(label,predict)
    val dfr = df.rdd.map{m => (m(0).toString.toDouble,m(1).toString.toDouble)}
    val arr =  new BinaryClassificationMetrics(dfr).roc.collect()
    import spark.sqlContext.implicits._
    val dfxy = spark.sparkContext.parallelize(arr.toSeq).toDF("x","y")
    DFUtils.exportcsv(dfxy,output_pt)
  }
}
