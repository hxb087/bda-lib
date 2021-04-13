package bda.spark.model.word2vec

import bda.common.Logging
import bda.common.linalg.DenseMatrix
import bda.common.obj.Doc
import bda.common.stat.Counter
import bda.common.util.{Timer, Msg}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

/**
  * Only implement the shared model
  */
private[word2vec] class Word2VecTrainer(val K: Int,
                                        val W: Int,
                                        val learn_rate: Double,
                                        val reg: Double,
                                        val max_window: Int,
                                        val max_iter: Int,
                                        val subsample_coef: Double,
                                        val n_negative: Int)
  extends Logging {

  protected var bc_util: Broadcast[W2VUtil] = null
  protected var sc: SparkContext = null

  def train(docs: RDD[Doc]): Word2VecModel = {
    var model = init(docs)

    val loss = computeLoss(model, docs)
    val msg = Msg("iter" -> 0,
      "loss" -> loss,
      "model" -> model.toString)
    logInfo(msg.toString)

    for (iter <- 1 to max_iter) {
      val timer = new Timer()
      model = update(model, docs, iter)
      val loss = computeLoss(model, docs)

      val msg = Msg("iter" -> iter,
        "loss" -> loss,
        "model" -> model.toString,
        "time cost(ms)" -> timer.cost())
      logInfo(msg.toString)
    }

    model
  }

  /** Initialize the state variables and a model */
  private def init(docs: RDD[Doc]): Word2VecModel = {
    sc = docs.context

    val tf = termFreq(docs)
    val N_w = docs.map(_.size).reduce(_ + _)

    val util = new W2VUtil(K, W, max_window, tf, subsample_coef, N_w, n_negative)
    bc_util = sc.broadcast(util)

    Word2VecModel.init(W, K)
  }

  /**
    * Count the term frequency in docs
    * @param docs  Each document is a word sequence.
    * @return A Term-frequency Map
    */
  private def termFreq(docs: RDD[Doc]): Map[Int, Int] = {
    val terms = docs.flatMap(_.words).collect()
    Counter(terms).toMap
  }

  /** A single iteration of parameter update */
  protected def update(model: Word2VecModel,
                       docs: RDD[Doc],
                       iter: Int): Word2VecModel = {
    val lrate = learn_rate // math.sqrt(iter)

    // update L
    val grad_L = collectGradientL(docs, model)
    model.L *= (1 - lrate * reg) += (grad_L * lrate)

    // update R
    val grad_R = collectGradientR(docs, model)
    model.R *= (1 - lrate * reg) += (grad_R * lrate)

    model
  }

  /** Collect the gradient of context gradient from a doc */
  private def collectGradientL(docs: RDD[Doc],
                               model: Word2VecModel): DenseMatrix[Double] = {
    val util = bc_util
    val bc_model = docs.context.broadcast(model)

    // compute  the gradient of L and R
    docs.mapPartitions { ds =>
      val grad_L = new DenseMatrix[Double](W, K)
      val R = bc_model.value.R
      ds.foreach { doc =>
        val words = util.value.subSampling(doc.words)
        for (i <- words.indices) {
          val w1 = words(i)

          val (start, end) = util.value.getContextWindow(i, words.length)
          for (j <- start to end if j != i) {
            val w2 = words(j)
            // positive sample update
            val y = bc_model.value.predict(w1, w2)
            grad_L(w1) += R(w2) * (1.0 - y)

            // negative sample update
            util.value.negSample(w2).foreach { w3 =>
              // positive sample update
              val y = model.predict(w1, w3)
              grad_L(w1) -= R(w3) * y
            }
          } // end j
        } // end i
      }
      Iterator(grad_L)
    }.treeReduce(_ + _)
  }

  /** Collect the gradient of context gradient from a doc */
  private def collectGradientR(docs: RDD[Doc],
                               model: Word2VecModel): DenseMatrix[Double] = {

    val util = bc_util
    val bc_model = docs.context.broadcast(model)

    // compute  the gradient of L and R
    docs.mapPartitions { ds =>
      val grad_R = new DenseMatrix[Double](W, K)
      val L = bc_model.value.L
      ds.foreach { doc =>
        val words = util.value.subSampling(doc.words)
        for (i <- words.indices) {
          val w1 = words(i)

          val (start, end) = util.value.getContextWindow(i, words.length)
          for (j <- start to end if j != i) {
            val w2 = words(j)
            // positive sample update
            val y = model.predict(w1, w2)
            grad_R(w2) += L(w1) * (1.0 - y)

            // negative sample update
            util.value.negSample(w2).foreach { w3 =>
              // positive sample update
              val y = model.predict(w1, w3)
              grad_R(w3) -= L(w1) * y
            }
          } // end j
        } // end i
      }

      Iterator(grad_R)
    }.treeReduce(_ + _)
  }

  /**
    * Compute the negative log-likelihood over docs
    */
  protected def computeLoss(model: Word2VecModel,
                            docs: RDD[Doc]): Double = {
    val util = bc_util.value
    val bc_model = docs.context.broadcast(model)

    docs.mapPartitions { ds =>
      var loss = 0.0
      val model = bc_model.value
      ds.foreach { doc =>
        val words: Array[Int] = util.subSampling(doc.words)

        /** Compute the loss of each document */
        for (i <- words.indices) {
          val w1 = words(i)
          val (start, end) = util.getContextWindow(i, words.length)
          for (j <- start to end if j != i) {
            val w2 = words(j)
            val y = model.predict(w1, w2)
            loss += (1 - y)

            util.negSample(w2).foreach { w3 =>
              val y = model.predict(w1, w3)
              loss += y
            }
          } // end j
        } // end i
      }
      Iterator(loss)
    }.sum() / (docs.count() * n_negative * max_window * 2)
  }
}


/**
  * A util class, which should broadcast to each node
  * @param K  dimension of latent features
  * @param W  vocabulary size
  * @param max_window   maximum context window size
  * @param tf  term frequency table
  * @param subsample_coef   parameter for sub-sampling
  * @param N_w  total number of word occurrences
  * @param n_negative  fold of negative samples
  */
private[word2vec]
class W2VUtil(val K: Int,
              val W: Int,
              val max_window: Int,
              val tf: Map[Int, Int],
              val subsample_coef: Double,
              val N_w: Int,
              val n_negative: Int)
  extends Serializable {

  val word_table = createUnigramTable()
  private val TABLE_SIZE: Int = 1e8.toInt

  /**
    * Sample the words in a document
    */
  def subSampling(words: Array[Int]): Array[Int] = {
    words.filter { w =>
      val v = (math.sqrt(tf(w) / (subsample_coef * N_w)) + 1) * (subsample_coef * N_w) / tf(w)
      v > math.random
    }
  }

  /**
    * Word probability for negative sampling
    */
  def createUnigramTable(): Array[Int] = {
    val unigramTable = new Array[Int](TABLE_SIZE)
    val power: Double = 0.75
    val train_words_pow = tf.map(x => math.pow(x._2, power)).sum

    var i: Int = 0
    var d1 = math.pow(tf(i), power) / train_words_pow
    for (a <- 0 until TABLE_SIZE) {
      unigramTable(a) = i
      if (a / TABLE_SIZE.toDouble > d1) {
        i = i + 1
        d1 += math.pow(tf(i), power) / train_words_pow
      }
    }
    unigramTable
  }

  /**
    * Sample negative samples
    * @param w  a positive word
    * @return  A sample sequence with one positive sample and
    *          multiple negative samples
    */
  def negSample(w: Int): Seq[Int] = {
    val N = word_table.length
    (0 until n_negative).map { i =>
      word_table((math.random * N).toInt)
    }.filter(_ != w)
  }

  /**
    * Compute the start and end indexes of a context window with random size
    * @param i  central index
    * @param max_i   maximum index
    * @return the (start, end) of the context window
    */
  def getContextWindow(i: Int, max_i: Int): (Int, Int) = {
    val window = (math.random * max_window).toInt + 1
    val start = math.max(0, i - window)
    val end = math.min(max_i - 1, i + window)
    (start, end)
  }
}


