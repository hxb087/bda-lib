package bda.spark.preprocess

import bda.common.obj.{RawDoc, RawPoint}
import bda.common.stat.Counter
import org.apache.spark.rdd.RDD

/** TFIDF statistic and transformation */
object TFIDF {

  /**
    * Transform docs from word sequence into tfidf representation
    *
    * @param docs   Each docs is a word Array
    * @param idf    The idf values of each word
    * @return     A document sequence, each document is represent by a sequence
    *             of word-tfidf value tuple
    */
  def apply(docs: RDD[RawDoc],
            idf: Map[String, Double]): RDD[RawPoint] = {

    val bc_idf = docs.context.broadcast(idf)
    docs.map { doc =>
      val fs = Counter(doc.words).toSeq.filter {
        // filter words without idf values
        case (w, tf) => bc_idf.value.contains(w)
      }.map {
        case (w, tf) => (w, tf * bc_idf.value(w))
      }.toMap
      RawPoint(doc.id, doc.label, fs)
    }
  }

  /**
    * Stat idf of words in the documents, and then transform them into tfidf.
    *
    * @return  A tfidf representation of the documents, and a idf-value Map
    */
  def apply(docs: RDD[RawDoc]):
  (RDD[RawPoint], Map[String, Double]) = {
    val idf = statIDF(docs)
    val new_docs = apply(docs, idf)
    (new_docs, idf)
  }


  /**
    * Compute idf of words in a document collection
    */
  def statIDF(docs: RDD[RawDoc]): Map[String, Double] = {
    val D = docs.count()
    val w_dfs: Array[(String, Int)] = docs.flatMap { doc =>
      doc.words.distinct.map { w =>
        (w, 1)
      }
    }.reduceByKey(_ + _).collect
    w_dfs.map {
      case (w, df) =>
        val idf = math.log((D + 1.0) / (df + 1.0))
        (w, idf)
    }.toMap
  }
}