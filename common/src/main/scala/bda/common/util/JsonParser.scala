package bda.common.util

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.util.{Failure, Success, Try}

/**
 * @author: ljf
 * @date: 2020/11/23 17:27
 * @description: TODO
 * @modified By:
 * @version: $ 1.0
 */
class JsonParser[T] {

  /**
   * 泛型解决json匹配通用问题
   *
   * @param jsonStr ,TODO: 适配不同的输入
   * @return
   */
  //  def parser[T](jsonStr: String)(implicit m: Manifest[JsonParser[T]]): T = {
  //    extractFrom(jsonStr) match {
  //      case Success(jsonParsed) =>
  //        jsonParsed
  //      case Failure(exc) =>
  //        throw new IllegalArgumentException(exc)
  //    }
  //  }
  //
  //
  //  private def extractFrom[T](jsonString: String)(implicit m: Manifest[T]): Try[T] = {
  //    implicit val formats: DefaultFormats.type = DefaultFormats
  //
  //    Try {
  //      JsonMethods.parse(jsonString, useBigDecimalForDouble = true).extract[T]
  //    }
  //  }
  def parse(jsonStr: String)(implicit m: Manifest[T]): T = {
    extractFrom(jsonStr) match {
      case Success(jsonParsed) =>
        jsonParsed
      case Failure(exc) =>
        throw new IllegalArgumentException(exc)
    }
  }

  private def extractFrom(jsonString: String)(implicit m: Manifest[T]): Try[T] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    Try {
      JsonMethods.parse(jsonString, useBigDecimalForDouble = true).extract[T]
    }
  }

}

object JsonParser {

  import org.json4s.Extraction
  import org.json4s.jackson.JsonMethods._
  import org.json4s.DefaultFormats

  implicit lazy val serializerFormats: DefaultFormats.type = DefaultFormats

  def toJson[T](caseObject: T): String = {
    val jValue = Extraction.decompose(caseObject)
    val jsonString = compact(jValue)
    jsonString
  }
}
