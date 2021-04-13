package bda.common.util

/**
 * Rich operations for string
 */
object implicits {

  case class ParseOp[T](op: String => T)
  implicit val popDouble = ParseOp[Double](_.toDouble)
  implicit val popFloat = ParseOp[Float](_.toFloat)
  implicit val popInt = ParseOp[Int](_.toInt)
  implicit val popLong = ParseOp[Long](_.toLong)
  implicit val popBoolean = ParseOp[Boolean](_.toBoolean)
  implicit val popString = ParseOp[String](_)
}
