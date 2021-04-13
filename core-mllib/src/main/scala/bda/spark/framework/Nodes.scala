package bda.spark.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag

/**
 * A operate node in dataflow.
 */
abstract class Node extends Serializable {
}

/**
 * An Estimator transforms the original RDD to a key-values RDD
 */
abstract class Transformer extends Node {

  def transform(data: DataFrame): DataFrame
}

abstract class RDDTransformer[I, O] extends Serializable{
  def transform(data: RDD[I]): RDD[O]
}

/**
 * A RecordTransformer is one-to-one Mapping of the original records
 */
abstract class RecordTransformer[I, O : ClassTag] extends RDDTransformer[I, O] {

  /** Transform a record **/
  def transform(rd: I): O

 def transform(data: RDD[I]): RDD[O] =
    data.map(rd => transform(rd))
}

//class DataFrameSaver[T <: scala.Product : TypeTag](rds: RDD[T]) extends Node with Logging {
//
//  def save(pt: String) {
//    logInfo("save dataframe to:" + pt)
//    val sqlContext = new SQLContext(rds.context)
//    import sqlContext.implicits._
//    rds.toDF().write.mode("overwrite").parquet(pt)
//  }
//}

//class DataFrameLoader(sc: SparkContext) extends Node with Logging {
//
//  def load(pt: String): DataFrame = {
//    logInfo("load dataframe from:" + pt)
//    new SQLContext(sc).read.parquet(pt)
//  }
//}

//abstract class Evaluator[T] extends Node {
//
//  /**
//   * Evaluate the predictions `ys`
//   */
//  def transform(labels: RDD[T], ys:RDD[T]): Unit
//}
