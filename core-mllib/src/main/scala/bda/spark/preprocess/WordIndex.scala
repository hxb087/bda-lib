package bda.spark.preprocess

import bda.common.obj.{Doc, RawDoc}
import org.apache.spark.rdd.RDD

object WordIndex {

  /**
    * Transform words in each document into indexes
    * @param docs  Each document is a sequence of words
    * @param w2id  word-index Map
    * @return  A document collection represented by word indexes
    */
  def apply(docs: RDD[RawDoc],
            w2id: Map[String, Int]): RDD[Doc] = {
    val bc_w2id = docs.context.broadcast(w2id)
    docs.map { doc =>
      // filter out unindexed words
      val ws = doc.words.filter(bc_w2id.value.contains(_)).map { w =>
        bc_w2id.value(w)
      }
      new Doc(doc.id, doc.label, ws)
    }
  }

  /**
    * Indexed words in the document collection, and transform words in
    * the documents into indexes.
    * @param docs  The original input document collection.
    * @param filter_words  Words to be filtered out, like stop words
    * @return   (new_docs, w2id)
    */
  def apply(docs: RDD[RawDoc],
            filter_words: Set[String] = Set.empty[String]
           ): (RDD[Doc], Map[String, Int]) = {
    val w2id = fit(docs, filter_words)
    val new_docs = apply(docs, w2id)
    (new_docs, w2id)
  }

  /**
    * Index words in the document collection
    * @param filter_words  Words to be filtered out, like stop words*
    * @return  A word-index map
    */
  def fit(docs: RDD[RawDoc],
          filter_words: Set[String]
         ): Map[String, Int] =
    docs.flatMap { doc =>
      doc.words.distinct.filter{ w =>
        !filter_words.contains(w)
      }
    }.distinct().collect().zipWithIndex.toMap
}