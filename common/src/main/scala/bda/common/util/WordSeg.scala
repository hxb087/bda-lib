package bda.common.util

import org.ansj.domain.Term
import org.ansj.splitWord.analysis.ToAnalysis

/**
  * Chinese word segment using Ansj
  */
object WordSeg {

  /** Segment a String into a word sequence */
  def apply(content: String, tagged: Boolean = false): Array[String] =
    ToAnalysis.parse(content).getTerms.toArray.map {
    case t: Term =>
      if (tagged) t.toString else t.getName
  }
}
