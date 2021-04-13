package com.golaxy.bda.statistics.analysis.common

/**
 * @author ：ljf
 * @date ：2020/11/6 15:54
 * @description：特征分析工具
 * @modified By：
 * @version: $ 1.0
 */
object AnalysisUtils {
  val metricsMap = Map(
    "mean" -> "均值",
    "sum" -> "求和",
    "variance" -> "方差",
    "std" -> "标准差",
    "numNonZeros" -> "非零值总和",
    "max" -> "最大值",
    "min" -> "最小值",
    "normL2" -> "L2范数",
    "normL1" -> "L1范数",
    "range" -> "极差",
    "groupInterval" -> "组间距",
    "variationCoefficient" -> "变异系数",
    "quantile-0.25" -> "四分之一位数",
    "quantile-0.5" -> "四分之二位数",
    "quantile-0.75" -> "四分之三位数",
    "quantile-1.0" -> "四分之四位数",
    "mode" -> "众数",
    "median" -> "中位数",
    "skewness" -> "偏度",
    "kurtosis" -> "峰度"
  )
}
