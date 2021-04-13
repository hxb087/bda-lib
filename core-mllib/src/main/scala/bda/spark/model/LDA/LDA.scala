package bda.spark.model.LDA

import bda.common.Logging
import bda.common.linalg.DenseVector
import bda.common.obj.Doc
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Abstract class for LDA model on Spark
  */
abstract class LDAModel extends Logging {
  /** Number of topics */
  val K: Int

  /** Number of words */
  val W: Int

  /** Save the model into a file */
  def save(model_pt: String): Unit

  def logLikelihood(docs: RDD[Doc]): Double

  def predict(docs: RDD[Doc]): RDD[DenseVector[Double]]
}

object LDAModel extends Logging {

  /**
    * load a model from file
    * @param model_pt Model path
    * @param graphx If true, load a GraphX version model; Else load
    *               a Shared model.
    * @return
    */
  def load(sc: SparkContext,
           model_pt: String,
           graphx: Boolean): LDAModel = {
    if (graphx)
      LDAGraphXModel.load(sc, model_pt)
    else
      LDASharedModel.load(sc, model_pt)
  }
}

/**
  * Abstract class for LDA trainer on Spark
  */
abstract class LDATrainer[M <: LDAModel] extends Logging {

  /** Number of topics */
  val K: Int

  /** Dirichlet prior of P(w|z) */
  val alpha: Double

  /** Dirichlet prior of P(z|d) */
  val beta: Double

  /** Maximum number of iterations */
  val max_iter: Int

  /**
    * Iteratively train a model over input documents
    * @param docs  Each document is an Array of word indexes
    * @return
    */
  def train(docs: RDD[Doc]): M

}

object LDA {

  /**
    * Train a LDA Model
    * @param docs  Input documents, each one is a array of words
    * @param topic_num  topic number
    * @param alpha   Dirichlet prior of P(w|z). Default is 50 / topic_num
    * @param beta    Dirichlet prior of P(z|d)
    * @param max_iter   maximum number of iteration
    * @param graphx   If true, use the GraphX implementation;
    *                 Else use the shared model implementation.
    * @return  A LDAModel
    */
  def train(docs: RDD[Doc],
            topic_num: Int = 10,
            alpha: Double = -1.0,
            beta: Double = 0.01,
            max_iter: Int = 100,
            graphx: Boolean = false): LDAModel = {

    // set alpha to be 50/k if it is not set
    val a = if (alpha > 0) alpha else 50.0 / topic_num

    // determine the number of words if not set
    val W = docs.map(_.words.max).max + 1

    val trainer = if (graphx)
        new LDAGraphXTrainer(topic_num, a, beta, max_iter)
      else
        new LDASharedTrainer(topic_num, W, a, beta, max_iter)

    trainer.train(docs)
  }
}