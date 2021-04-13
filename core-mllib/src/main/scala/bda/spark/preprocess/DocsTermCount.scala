package bda.spark.preprocess

import bda.common.obj.RawDoc
import org.apache.spark.rdd.RDD


/**
  * Counter the term frequency in docs
  */
object DocsTermCount {
  /** Window size for biterm extraction */
  var bitrem_window_size: Int = 10

  /**
    * Counter the term frequency in docs
    *
    * @param docs     Each document is a word sequence.
    * @param termType   "unigram" or "bigram" or "biterm". The default window
    *                   size for biterm extraction is 10. If you want to chang
    *                   it, please set `biterm_window_size`.
    * @param isDocFreq  If true, count the document frequency of words; Else
    *                   count the overall term frequency of words.
    * @return   A Term-frequency Map
    */
  def apply(docs: RDD[RawDoc],
            termType: String = "unigram",
            isDocFreq: Boolean = false): Map[String, Int] = {

    val genTerms: (Seq[String] => Seq[String]) = termType match {
      case "unigram" => (a => a)
      case "bigram" => ws2bigrams _
      case "biterm" => ws2biterms _
      case other => throw new IllegalArgumentException(s"unsupported term type $other")
    }

    val terms = if (isDocFreq)
      docs.flatMap(doc =>  genTerms(doc.words).distinct)
    else
      docs.flatMap(doc => genTerms(doc.words))

    terms.map { t => (t, 1) }.reduceByKey(_ + _).collect().toMap
  }

  /** Extract bigrams from word sequence */
  private def ws2bigrams(ws: Seq[String]): Seq[String] = {
    if (ws.size < 2)
      Seq.empty[String]
    else {
      ws.sliding(2).map {
        case Seq(w1, w2) => s"$w1 $w2"
      }.toSeq
    }
  }

  /** Extract bigrams from word sequence */
  private def ws2biterms(ws: Seq[String]): Seq[String] = {
    if (ws.size < 2)
      Seq.empty[String]
    else {
      (0 until ws.size - 1).flatMap { i =>
        (i + 1 until math.min(i + bitrem_window_size, ws.size)).map { j =>
          if (ws(i) < ws(j))
            s"${ws(i)} ${ws(j)}"
          else
            s"${ws(j)} ${ws(i)}"
        }
      }.toSeq
    }
  }
}
