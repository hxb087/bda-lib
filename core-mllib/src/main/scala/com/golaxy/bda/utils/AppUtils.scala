package com.golaxy.bda.utils

import java.text.SimpleDateFormat

import org.apache.spark.ml.evaluation.{BDAEvaluator, BDAMulticlassClassificationEvaluator}

/**
 * Created by Administrator on 2020/3/12.
 * 应用提交工具类
 */
object AppUtils {

  /**
   *
   * @param name
   * @return
   * name 后面增加时间戳
   */
  def AppName(name: String): String = name.concat("-").concat(System.currentTimeMillis() + "")


  def main(args: Array[String]) {
    println(AppName("test"))
  }

  def getEvaluator(evalAlgo: String, metricName: String): BDAEvaluator = {
    evalAlgo match {
      case "classification" => new BDAMulticlassClassificationEvaluator().setMetricName(metricName)
//      case "regression" => TODO: 待实现回归算法指标类
      case _ => throw new UnsupportedOperationException(s"evalAlgo not support $evalAlgo," +
        s"implemented 'classification' and 'regression' ")
    }
  }
}
