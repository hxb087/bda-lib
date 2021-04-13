package bda.common.util

import bda.common.util.implicits.ParseOp
import org.apache.commons.lang3.StringEscapeUtils

/**
  * String util functions
 */
object StrUtil {

  /**
   * Parse a string to a value with specified type
 *
   * @tparam T  type of result value. Support Double, Float,
   *            Int, Long, Boolean, and String.
   * @example
   * {{{
   *     import bda.util.implicits._
   *     val v: Double = StrUtil.parse[Double]("1.0").getOrElse(0.0)
   * }}}
   */
  def parse[T: ParseOp](s: String): Option[T] = try {
    Some(implicitly[ParseOp[T]].op(s))
  } catch {
    case _: Throwable => None
  }

  /**
   * Parse a key-value vector from a string
 *
   * @param  s: "k:v ...."
   * @return Array[(k, v), ...]
   */
  def parseKVs(s: String,
               delim: String = " "): Array[(String, String)] =
    s.split(delim).map { kv =>
      val Array(k, v) = kv.split(":")
      (k, v)
    }

  /**
   * convert `s` to a string Map
 *
   * @param  s: "k:v ...."
   * @return {k:v, ...}
   */
  def parseMap(s: String,
               delim: String = " "): Map[String, String] =
    parseKVs(s, delim).toMap

  /**
   * Unescapes any Java literals found in the String.
   * For example, it will turn a sequence of '\' and 'n' into a newline character,
   *  unless the '\' is preceded by another '\'.
   *
   * @param s the String to unescape, may be null.
   * @return a new unescaped String, null if null string input.
   */
  def unescapeJava(s: String): String = {
    StringEscapeUtils.unescapeJava(s)
  }
}
