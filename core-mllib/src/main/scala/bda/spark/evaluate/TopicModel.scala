package bda.spark.evaluate

import bda.common.obj.Doc
import bda.spark.model.LDA.LDAModel
import org.apache.spark.rdd.RDD

object TopicModel {
  /**
    * Perplexity computation:
    * P(D|model) = exp{- \sum_d logP(ws_d |model) / N_w}
    * where N_w is the total number of word occurrence in D,
    * and
    * P(ws_d|model) = \prod_w \sum_z P(w|z)P(z|d)
    */
  def perplexity(model: LDAModel, docs: RDD[Doc]): Double = {
    // number of word occurrences
    val nw = docs.map(_.size).sum
    if (nw == 0)
      return 0.0
    val a = model.logLikelihood(docs) / nw
    math.exp(-a)
  }
}
