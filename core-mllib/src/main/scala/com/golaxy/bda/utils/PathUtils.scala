package com.golaxy.bda.utils

/**
  * Created by lijg on 2020/4/24.
  * 管理系统中涉及到的所有的存储路径
  */
object PathUtils {


  val MODELPATH = "model"
  val EVALUATION_REPORT = "_evaluation"
  val EVALUATION_REPORT_CONFUSIONMATRIX = "matrix"
  val EVALUATION_REPORT_INDEX = "index"
  val EVALUATION_REPORT_BINARY_DATA_FREQUENT = "frequent"
  val EVALUATION_REPORT_BINARY_INDEX = "index"
  val EVALUATION_REPORT_BINARY_PR = "pr"
  val EVALUATION_REPORT_BINARY_ROC = "roc"

  /**
    * 评估报告指标目录
    *
    * @param path
    * @return
    */
  def getEvaluationIndexPath(path: String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_INDEX)

  /**
    * 混淆矩阵目录
    * @param path
    * @return
    */
  def getEvaluationConfusionMatrixPath(path: String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_CONFUSIONMATRIX)


  /**
    * 二分类评估等频数据输出目录
    * @param path
    * @return
    */
  def EvaluationBinaryFrequentDataPath(path:String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_BINARY_DATA_FREQUENT)

  /**
    * 二分类指标数据
    * @param path
    * @return
    */
  def EvaluationBinaryIndexDataPath(path:String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_BINARY_INDEX)


  /**
    * PR曲线
    * @param path
    * @return
    */
  def EvaluationBinaryPRCurve(path:String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_BINARY_PR)


  /**
    * ROC曲线
    * @param path
    * @return
    */
  def EvaluationBinaryROCCurve(path:String) = path.concat(EVALUATION_REPORT).concat("/").concat(EVALUATION_REPORT_BINARY_ROC)
}
