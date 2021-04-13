package bda.spark.preprocess

import bda.common.obj.RawDoc
import bda.common.util.WordSeg
import org.apache.spark.rdd.RDD

/**
  * Chinese word segment using Ansj
  */
object DocWordSeg {

  /**
    * Segment and filter words with length less than 2
    * @param docs  unsegmented Chinese documents.
    * @param has_id whether has document ID
    * @param has_label whether has document Label
    * @param seperator String used to seperate fields.
    * @return  RDD of [[bda.common.obj.RawDoc]]
    */
  def apply(docs: RDD[String],
            tagged: Boolean = false,
            has_id: Boolean = true,
            has_label: Boolean = true,
            seperator: String = "\t"): RDD[RawDoc] = {
    docs.map(apply(_, tagged, has_id, has_label, seperator))
  }

  /** Segment a String into a word sequence */
  def apply(doc: String,
            tagged: Boolean,
            has_id: Boolean,
            has_label: Boolean,
            seperator: String): RawDoc = {

    (has_id, has_label) match {
      case (true, true) =>
        val Array(id, label, content) = doc.split(seperator, 3)
        val ws = WordSeg(content, tagged)
        new RawDoc(id, label.toDouble, ws)
      case (true, false) =>
        val Array(id, content) = doc.split(seperator, 2)
        val label = RawDoc.default_label
        val ws = WordSeg(content, tagged)
        new RawDoc(id, label, ws)
      case (false, true) =>
        val Array(label, content) = doc.split(seperator, 2)
        val id = RawDoc.default_id
        val ws = WordSeg(content, tagged)
        new RawDoc(id, label.toDouble, ws)
      case (false, false) =>
        val id = RawDoc.default_id
        val label = RawDoc.default_label
        val ws = WordSeg(doc, tagged)
        new RawDoc(id, label, ws)
    }
  }
}