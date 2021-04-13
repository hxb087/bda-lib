package bda.spark.model.LDA

import bda.common.Logging
import bda.common.linalg.mutable.SparseVector
import bda.common.linalg.{DenseMatrix, DenseVector}
import bda.common.obj.Doc
import bda.common.util.{Sampler, Timer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class LDASharedModel(val sc: SparkContext,
                     val alpha: Double,
                     val pwz: DenseMatrix[Double])
  extends LDAModel {

  /** If necessary, broadcast the model to each worker */
  lazy val bc_pwz = sc.broadcast(pwz)

  override val K = pwz.rowNum

  override val W = pwz.colNum

  /** Evaluate the likelihood of documents. It can
    * be used for Perplexity evaluation.
    */
  def logLikelihood(docs: RDD[Doc]): Double = {
    val pz_ds = predict(docs)
    val local_pwz = bc_pwz
    docs.zip(pz_ds).map {
      case (doc, pz_d) => doc.words.map { w =>
        math.log(pz_d dot local_pwz.value.col(w))
      }.sum
    }.sum
  }

  /**
    * Infer P(z|d) for documents
    *
    * @return  P(z|d), a K-dimension DenseVector
    */
  def predict(docs: RDD[Doc]): RDD[DenseVector[Double]] = {
    val local_K = K
    val pwz0 = bc_pwz
    val a = alpha
    docs.map { doc =>
      // initial topic assignments
      val ws = doc.words
      val zs: Array[Int] = ws.map(w => Sampler.uniform(local_K))
      val nz_d = SparseVector.count(zs).toDenseVector(local_K)

      // fix the number of iteration to 10
      for (iter <- 1 to 10) {
        for (i <- ws.indices) {
          val w = ws(i)
          // reset topic assignment
          val old_z = zs(i)
          nz_d(old_z) -= 1
          // draw topic assignment
          val pz_dw = (nz_d.toDouble + a) * pwz0.value.col(w)
          val z = Sampler.multinomial(pz_dw)
          // update topic assignment records
          zs(i) = z
          nz_d(z) += 1
          assert(nz_d.values.forall(_ >= 0))
        }
      }
      val pz_d = (nz_d.toDouble + a).normalize()
      pz_d
    }
  }

  /** Output tow files: model.priors and model.nwz */
  override def save(model_pt: String): Unit = {
    logInfo("write model into: " + model_pt)
    val alpha_pt = model_pt + "model.alpha"
    val pwz_pt = model_pt + "model.pwz"
    sc.makeRDD(Seq(alpha), 1).saveAsObjectFile(alpha_pt)
    sc.makeRDD(Seq(pwz), 1).saveAsObjectFile(pwz_pt)
  }
}

object LDASharedModel extends Logging {

  /** Load a LDASharedModel from file */
  def load(sc: SparkContext, model_pt: String): LDASharedModel = {
    logInfo("Load model from: " + model_pt)
    val alpha_pt = model_pt + "model.alpha"
    val pwz_pt = model_pt + "model.pwz"
    val alpha = sc.objectFile[Double](alpha_pt).first()
    val pwz = sc.objectFile[DenseMatrix[Double]](pwz_pt).first()
    new LDASharedModel(sc, alpha, pwz)
  }
}

/**
  * Train LDA with Gibbs-EM algorithm with the model shared among workers.
 *
  * @param K   Number of topics
  * @param W   Number of words
  * @param alpha  Dirichlet prior of P(w|z)
  * @param beta   Dirichlet prior of P(z|d)
  * @param max_iter  Maximum number of iterations
  */
class LDASharedTrainer(override val K: Int,
                       val W: Int,
                       override val alpha: Double,
                       override val beta: Double,
                       override val max_iter: Int)
  extends LDATrainer[LDASharedModel] {

  def train(docs: RDD[Doc]): LDASharedModel = {
    var dzs = initTopicAssignments(docs)
    var nwz = countNwz(dzs)

    val times = new ArrayBuffer[Double]()
    for (iter <- 0 until max_iter) {
      val timer = new Timer()
      val pre_dzs = dzs

      dzs = updateTopicAssignments(dzs, nwz).cache()
      dzs.foreachPartition(x => Unit) // materialize rdd
      pre_dzs.unpersist()

      // update nwz
      nwz = countNwz(dzs)

      val cost_time = timer.cost()
      times.append(cost_time)
      logInfo(s"iter $iter, time:$cost_time ms")
    }

    logInfo(s"average iteration time: " + times.sum / times.size + "ms")
    val pwz = (nwz.toDouble + beta).normalizeRow()
    new LDASharedModel(docs.context, alpha, pwz)
  }

  /**
    * Initialize the topic assignments for each word
 *
    * @param docs  Input document collection
    * @return  RDD[(ws: Array[Int], zs: Array[Int])]
    */
  def initTopicAssignments(docs: RDD[Doc]): RDD[(Array[Int], Array[Int])] = {
    val local_K = K
    docs.map { doc =>
      val zs = doc.words.map(w => Sampler.uniform(local_K))
      (doc.words, zs)
    }.cache()
  }

  /**
    * Statistic the number of times that each word assigned to each topic
    * in current topic assignments, i.e., nwz
    *
    * @param dzs    the words and topic assignments of each document
    * @return   A DenseMatrix[Int]
    */
  private def countNwz(dzs: RDD[(Array[Int], Array[Int])]): DenseMatrix[Int] = {
    val local_K = K
    val local_W = W
    dzs.mapPartitions { p_dzs =>
      val nwz = new DenseMatrix[Int](local_K, local_W)
      p_dzs.foreach {
        case (ws, zs) => ws.zip(zs).foreach {
          case (w, z) => nwz(z, w) += 1
        }
      }
      Iterator(nwz)
    }.reduce(_ + _)
  }

  /**
    * Recompute the topic assignments for each word with a fixed model
 *
    * @param dzs  documents with topic assignments
    * @param nwz  the old topic assignments
    * @return  A new P(z|d)
    */
  private def updateTopicAssignments(dzs: RDD[(Array[Int], Array[Int])],
                                     nwz: DenseMatrix[Int]):
  RDD[(Array[Int], Array[Int])] = {
    val pwz = (nwz.toDouble + beta).normalizeRow()
    val bc_pwz = dzs.context.broadcast(pwz)
    val local_K = K
    val a = alpha
    dzs.map {
      case (ws, zs) =>
        val nz_d = SparseVector.count(zs)
        // Re-sample a topic for each word without update the model
        val new_zs = ws.zip(zs).map {
          case (w, z) =>
            nz_d(z) -= 1
            val pz = (nz_d.toDenseVector(local_K).toDouble + a) * bc_pwz.value.col(w)
            val new_z = Sampler.multinomial(pz)
            nz_d(z) += 1
            new_z
        }
        (ws, new_zs)
    }
  }
}
