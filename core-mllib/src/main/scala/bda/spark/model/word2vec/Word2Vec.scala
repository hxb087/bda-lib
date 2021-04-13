package bda.spark.model.word2vec

import bda.common.Logging
import bda.common.obj.Doc
import bda.common.util.Msg
import org.apache.spark.rdd.RDD

/**
  * External interface of word2vec
  */
object Word2Vec extends Logging {

  /**
    * Train a word2vec model
    * @param docs input documents, each one is a array of words
    * @param K   dimension of the word vectors
    * @param learn_rate learning rate
    * @param max_window maximum size of context window
    * @param max_iter number of iteration
    * @param subsample_coef coefficient of sub-sampling
    * @param n_negative  folds of negative samples
    * @return a word2vec model
    */
  def train(docs: RDD[Doc],
            w2id: Map[String, Int],
            K: Int = 100,
            learn_rate: Double = 0.001,
            reg: Double = 0.01,
            max_window: Int = 3,
            max_iter: Int = 15,
            subsample_coef: Double = 1e-2,
            n_negative: Int = 15
           ): Word2VecModel = {

    // logging the input parameters
    val msg = Msg(
      "n(doc)" -> docs.count(),
      "K" -> K,
      "learn_rate" -> learn_rate,
      "reg" -> reg,
      "max_window" -> max_window,
      "max_iter" -> max_iter,
      "coef(sub_sample)" -> subsample_coef,
      "negative fold" -> n_negative
    )
    logInfo(msg.toString)

    val W = docs.map(_.words.max).max + 1

    val trainer = new Word2VecTrainer(K, W, learn_rate,
      reg, max_window,
      max_iter, subsample_coef, n_negative)

    val model: Word2VecModel = trainer.train(docs)
    model.normalize()
  }
}

