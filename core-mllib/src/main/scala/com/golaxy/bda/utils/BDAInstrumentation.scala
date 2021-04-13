package org.apache.spark.ml.util


import java.util.UUID

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
 * @author ：ljf
 * @date ：2020/10/16 18:39
 * @description：enhance instrumentation
 * @modified By：
 * @version: $ 1.0
 */

private[spark] class BDAInstrumentation private() extends Logging {

  private val id = UUID.randomUUID()
  private val shortId = id.toString.take(8)
  private[util] val prefix = s"[$shortId] "

  /**
   * Log some info about the pipeline stage being fit.
   */
  def logPipelineStage(stage: PipelineStage): Unit = {
    // estimator.getClass.getSimpleName can cause Malformed class name error,
    // call safer `BDAUtils.getSimpleName` instead
    val className = stage.getClass.getSimpleName
    //    val className = BDAUtils.getSimpleName(stage.getClass)
    logInfo(s"Stage class: $className")
    logInfo(s"Stage uid: ${stage.uid}")
  }

  /**
   * Log some data about the dataset being fit.
   */
  def logDataset(dataset: Dataset[_]): Unit = logDataset(dataset.rdd)

  /**
   * Log some data about the dataset being fit.
   */
  def logDataset(dataset: RDD[_]): Unit = {
    logInfo(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  /**
   * Logs a debug message with a prefix that uniquely identifies the training session.
   */
  override def logDebug(msg: => String): Unit = {
    super.logDebug(prefix + msg)
  }

  /**
   * Logs a warning message with a prefix that uniquely identifies the training session.
   */
  override def logWarning(msg: => String): Unit = {
    super.logWarning(prefix + msg)
  }

  /**
   * Logs a error message with a prefix that uniquely identifies the training session.
   */
  override def logError(msg: => String): Unit = {
    super.logError(prefix + msg)
  }

  /**
   * Logs an info message with a prefix that uniquely identifies the training session.
   */
  override def logInfo(msg: => String): Unit = {
    super.logInfo(prefix + msg)
  }

  /**
   * Logs the value of the given parameters for the estimator being used in this session.
   */
  def logParams(hasParams: Params, params: Param[_]*): Unit = {
    val pairs: Seq[(String, JValue)] = for {
      p <- params
      value <- hasParams.get(p)
    } yield {
      val cast = p.asInstanceOf[Param[Any]]
      p.name -> parse(cast.jsonEncode(value),false)
    }
    logInfo(compact(render(map2jvalue(pairs.toMap))))
  }

  def logNumFeatures(num: Long): Unit = {
    logNamedValue(BDAInstrumentation.loggerTags.numFeatures, num)
  }

  def logNumClasses(num: Long): Unit = {
    logNamedValue(BDAInstrumentation.loggerTags.numClasses, num)
  }

  def logNumExamples(num: Long): Unit = {
    logNamedValue(BDAInstrumentation.loggerTags.numExamples, num)
  }

  /**
   * Logs the value with customized name field.
   */
  def logNamedValue(name: String, value: String): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Long): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Double): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Array[String]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toSeq)))))
  }

  def logNamedValue(name: String, value: Array[Long]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toSeq)))))
  }

  def logNamedValue(name: String, value: Array[Double]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toSeq)))))
  }


  /**
   * Logs the successful completion of the training session.
   */
  def logSuccess(): Unit = {
    logInfo("training finished")
  }

  /**
   * Logs an exception raised during a training session.
   */
  def logFailure(e: Throwable): Unit = {
    val msg = e.getStackTrace.mkString("\n")
    super.logError(msg)
  }
}

private[spark] object BDAInstrumentation {

  object loggerTags {
    val numFeatures = "numFeatures"
    val numClasses = "numClasses"
    val numExamples = "numExamples"
    val meanOfLabels = "meanOfLabels"
    val varianceOfLabels = "varianceOfLabels"
  }

  def instrumented[T](body: (BDAInstrumentation => T)): T = {
    val instr = new BDAInstrumentation()
    Try(body(instr)) match {
      case Failure(NonFatal(e)) =>
        instr.logFailure(e)
        throw e
      case Success(result) =>
        instr.logSuccess()
        result
    }
  }
}
