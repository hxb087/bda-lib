package org.apache.spark.ml

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object PipeUtils {
  def exportTotalStage(outPaht: String, spark: SparkSession, pipeStages: Array[Transformer], model: PipelineModel) = {

    val stagesthis = model.stages
    val pipeline = new Pipeline().setStages(pipeStages ++ stagesthis)
    val modelpip = new PipelineModel(pipeline.uid,pipeStages ++ stagesthis)
    modelpip.write.overwrite().save(outPaht + "Stages")
  }

  def loadBeforeStages(spark:SparkSession, inputPath:String): Array[Transformer] = {
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val stagePath = inputPath + "Stages"
    val exits = fs.exists(new Path(stagePath))
    var pipeStages = Array[Transformer]()
    if (exits) {
      pipeStages = PipelineModel.load(stagePath).stages
    }
    pipeStages
  }
}
