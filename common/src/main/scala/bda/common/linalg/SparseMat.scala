package bda.common.linalg

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/**
  * Sparse matrix that each row is a SparseVector.
  */
private[linalg] abstract
class SparseMat[T: Numeric : ClassTag](override val _mat: Vector[SparseVec[T]])
  extends Mat[T](_mat) {

  def this(mat: Seq[SparseVec[T]]) = this(mat.toVector)

  /** Return the number of active elements */
  def activeSize: Int = _mat.map(_.activeSize).sum

  /** Mean of all the active values */
  def activeMean: Double = {
    if (activeSize == 0)
      0.0
    else
      sum.toDouble() / activeSize
  }

  /** return the ith row of this matrix */
  def row(i: Int): SparseVec[T] = {
    require(i < rowNum, s"row $i exceeds the rowNum $rowNum")
    _mat(i)
  }

  /** Return the ith column */
  def col(i: Int): DenseVector[T] = new DenseVector(mapRow(c => c(i)))

  /** return the rows of this matrix */
  def rows: Vector[SparseVec[T]] = _mat

  /** alias of row(i) */
  def apply(i: Int): SparseVec[T] = row(i)

  /** Generate a new SparseMat by applying a function over the active (i,j,v) tuples */
  def mapActive[V: Numeric : ClassTag](f: (Int, Int, T) => V): SparseMat[V]

  /** Create a new SparseMat by applying f on each active value */
  def mapActiveValues[V: Numeric : ClassTag](f: T => V): SparseMat[V]

  /** Apply a function to each row */
  def mapRow[V](f: SparseVec[T] => V): Vector[V] = _mat.map(f)

  /**
    * Create a new SparseMat by p-norm normalization of this SparseMatrix
    *
    * @param p   p=1, sum-1; p=2, square-sum-1.
    * @return  A normalized SparseMat
    */
  def normalize(p: Int = 1): SparseMat[Double] = {
    val s = p match {
      case 1 => this.sum.toDouble()
      case 2 => this.norm
      case _ => throw new IllegalArgumentException(s"Unsupport $p norm normalization")
    }

    if (s == 0.0)
      throw new RuntimeException("normalization: sum is 0")
    this.mapActiveValues(_.toDouble / s)
  }

  /** Create a Double SparseMat from this SparseMatrix */
  def toDouble: SparseMat[Double]

  /** Create a new SparseMatrix using the negative of existing values */
  def unary_-(): SparseMat[T] = this.mapActiveValues(-_)

  /** Product a constant to all the elements */
  def *(x: T): SparseMat[T] = this.mapActiveValues(_ * x)

  /** Divide a constant to all the elements */
  def /(x: T): SparseMat[T] = {
    require(x != 0.0)
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        mapActiveValues { v => num.div(v, x) }
      case _ =>
        throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Element-wise equality comparison */
  def equal(that: SparseMat[T]): Boolean = {
    if (this.rowNum != that.rowNum) {
      false
    } else {
      (0 until rowNum).forall { i =>
        this.row(i) equal that.row(i)
      }
    }
  }

  /** Create a new SparseMat by adding another SparseMat */
  def +(that: SparseMat[T]): SparseMat[T]

  /** Create a new SparseVec by subtracting another SparseVec */
  def -(that: SparseMat[T]): SparseMat[T]

  /** Create a new SparseVec by element-wise multiplying another SparseVec */
  def *(that: SparseMat[T]): SparseMat[T]

  /** Create a DenseMatrix by adding a DenseVector to this */
  def +(that: DenseMatrix[T]): DenseMatrix[T] = {
    require(this.rowNum == that.rowNum)
    that + this
  }

  /** Create a DenseMatrix by adding a DenseMatrix to this */
  def -(that: DenseMatrix[T]): DenseMatrix[T] = {
    require(this.rowNum == that.rowNum)
    -that + this
  }

  /** Create a new SparseMat by element-wise multiplying a DenseVector */
  def *(that: DenseMatrix[T]): SparseMat[T] = {
    require(this.rowNum == that.rowNum)
    this.mapActive {
      case (i, j, v) => v * that(i, j)
    }
  }

  /** Create a new SparseMat by element-wise dividing a DenseVector */
  def /(that: DenseMatrix[T]): SparseMat[T] = {
    require(this.rowNum == that.rowNum)
    require(that.forall(_ != zero), "Divide a DenseVector with zero")
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        this.mapActive {
          case (i, j, v) => num.div(v, that(i, j))
        }
      case _ =>
        throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /**
    * Dot each row of this matrix with a DenseVector
    * @return A DenseVector whose size is the row number of this matrix
    */
  def dot(that: DenseVector[T]): DenseVector[T] = {
    new DenseVector(mapRow(_ dot that))
  }

  /**
    * Dot each row of this matrix with a SparseVector
    * @return A DenseVector whose size is the row number of this matrix
    */
  def dot(that: SparseVec[T]): DenseVector[T] = {
    new DenseVector(mapRow(_ dot that))
  }

  /**
    * Matrix production with a DenseMatrix.
    * The common matrix production manner is as:
    * A(N,K) * B(K,M) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(k,j).
    *
    * However, since our matrix is stored in row-based, the above
    * computation requires to access each column of B, which might be
    * inefficient.
    * To save the computation, we take a Row-based Matrix product
    * manner:
    * A(N,K) * B(M,K) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(j,k).
    *
    * @return  a DenseMatrix
    */
  def dot(that: DenseMatrix[T]): DenseMatrix[T] = {
    val mat: Vector[DenseVector[T]] = this.mapRow(_ dot that)
    new DenseMatrix(mat)
  }

  /**
    * Row-based Matrix dot product with another SparseMatrix:
    * A(N,K) * B(M,K) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(j,k).
    *
    * @return  a DenseMatrix
    */
  def dot(that: SparseMat[T]): DenseMatrix[T] = {
    val mat: Vector[DenseVector[T]] = this.mapRow(_ dot that)
    new DenseMatrix(mat)
  }
}

