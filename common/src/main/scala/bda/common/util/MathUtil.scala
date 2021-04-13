package bda.common.util
import org.apache.commons.math3.distribution.{PoissonDistribution => PD}

/**
  * Implementation of the Poisson distribution.
  *
  * @param p Specified mean of the Poisson distribution.
  */
class PoissonDistribution(p: Double) {

  /** generate an instance of Poisson distribution */
  val pd = new PD(p)

  /**
    * Generate a random value sampled from this distribution.
    *
    * @return the random value sampled from this distribution.
    */
  def sample(): Int = {

    pd.sample()
  }

  /**
    * Get the mean for the distribution.
    *
    * @return specified mean of the Poisson distribution.
    */
  def getMean: Double = pd.getMean
}

object MathUtil {

  /** Compute the Sigmoid without the sigTable */
  def Sigmoid(x: Double) = 1.0 / (1 + math.exp(-x))

}

/**
  * Sigmoid table to speed up Sigmoid computation
  * The precision is 2*MAX_EXP/EXP_TABLE_SIZE
  */
object Sigmoid {

  /**
    * the size of exp table which will be calculate in function createExpTable() and lookup later
    */
  private val EXP_TABLE_SIZE = 1000
  /**
    * correspond to exp(-6) to exp(6)
    */
  private val MAX_EXP: Double = 6.0
  private val sigTable = createSigTable()

  def apply(x: Double): Double = {
    if (x > Sigmoid.MAX_EXP)
      1.0 - Double.MinPositiveValue
    else if (x < -Sigmoid.MAX_EXP)
      Double.MinPositiveValue
    else
      sigTable(((x / Sigmoid.MAX_EXP + 1) * (Sigmoid.EXP_TABLE_SIZE / 2.0)).toInt)
  }

  /**
    *
    * @return exponential value table
    */
  private def createSigTable(): Array[Double] = {
    val sigTable = new Array[Double](EXP_TABLE_SIZE)
    for (i <- 0 until EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      sigTable(i) = tmp / (tmp + 1.0)
    }
    sigTable
  }
}
