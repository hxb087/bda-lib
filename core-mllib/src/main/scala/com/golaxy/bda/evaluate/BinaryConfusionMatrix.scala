package com.golaxy.bda.evaluate

/**
  * Created by lucky on 2020/5/26.
  */
trait BinaryConfusionMatrix {
  /** number of true positives */
  def numTruePositives: Long

  /** number of false positives */
  def numFalsePositives: Long

  /** number of false negatives */
  def numFalseNegatives: Long

  /** number of true negatives */
  def numTrueNegatives: Long

  /** number of positives */
  def numPositives: Long = numTruePositives + numFalseNegatives

  /** number of negatives */
  def numNegatives: Long = numFalsePositives + numTrueNegatives
}

case class BinaryConfusionMatrixImpl(
                                                          count: BinaryLabelCounter,
                                                          totalCount: BinaryLabelCounter) extends BinaryConfusionMatrix {

  /** number of true positives */
  override def numTruePositives: Long = count.numPositives

  /** number of false positives */
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives */
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives */
  override def numNegatives: Long = totalCount.numNegatives


}

