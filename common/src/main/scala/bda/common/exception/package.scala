package bda.common

/**
 * @author: ljf
 * @date: 2020/11/24 16:39
 * @description: 自定义异常
 * @modified By: 
 * @version: $ 1.0
 */
package object exception {

  final case class KeyNotFoundException(private val message: String = "",
                                   private val cause: Throwable = None.orNull) extends Exception(message, cause)

}
