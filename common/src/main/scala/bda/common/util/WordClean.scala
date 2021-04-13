package bda.common.util

/**
 * filter noisy chars in a English string
 */
object EnglishWordClean {

  /**
   * Extract words:
   * - only contains alphabet, digits, and "-"
   * - with more than 2 letters
   * and turn to lower-case.
   * @param s raw string
   * @return words separated by " "
   */
  def apply(s: String): String =
    "[a-z0-9\\-]{2,}".r.findAllIn(s.toLowerCase).mkString(" ")
}

/** todo */
object ChineseWordClean {

}