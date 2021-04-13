package bda.spark.model.word2vec

import bda.common.linalg.{DenseMatrix, DenseVector}
import bda.common.util.Sigmoid
import bda.common.util.io.{writeObject, readObject}
import scala.util.Random


/**
  * The same model as local
  */
class Word2VecModel(val L: DenseMatrix[Double],
                    val R: DenseMatrix[Double])
  extends Serializable {

  /** Vocabulary size */
  val W = L.rowNum

  /** Dimension of the word vector */
  val K = L.colNum

  /** For debug */
  override def toString: String = s"norm(L)=${L.norm}, norm(R)=${R.norm}"

  def getOutputVector(w: Int): DenseVector[Double] = R(w)

  /**
    * Predict the probability of a word occurrence
    * @param w1  central word
    * @param w2  context word
    * @return  The probability that the word to occurr
    */
  def predict(w1: Int, w2: Int): Double = {
    assert(w1 < W && w2 < W)
    Sigmoid(L(w1) dot R(w2))
  }

  /** compute the cosine similarity of two words */
  def similarity(w1: Int, w2: Int): Double = {
    val v1 = getInputVector(w1)
    val v2 = getInputVector(w2)
    v1.cos(v2)
  }

  /** Return the context vector representation of a word */
  def getInputVector(w: Int): DenseVector[Double] = L(w)

  /** Normalize the vector of each word */
  def normalize(): Word2VecModel = {
    L.normalize()
    R.normalize()
    this
  }

  /** Save the model in binary form */
  def save(pt: String): Unit =
    writeObject(pt, this)

}

/** Factory methods for Word2VecModel */
object Word2VecModel {

  /**
    * Random initialize a model
    * @param W  word number
    * @param K  dimension of the word vector
    * @return  A Word2VecModel
    */
  def init(W: Int, K: Int): Word2VecModel = {
    val L = DenseMatrix.fill[Double](W, K) {
      (Random.nextDouble() - 0.5) / K
    }

    val R = DenseMatrix.fill[Double](W, K) {
      (Random.nextDouble() - 0.5) / K
    }
    new Word2VecModel(L, R)
  }

  /** Load a model from a binary file */
  def load(pt: String): Word2VecModel = {
    readObject[Word2VecModel](pt)
  }
}


