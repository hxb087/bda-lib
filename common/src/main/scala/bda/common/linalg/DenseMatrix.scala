package bda.common.linalg

import bda.common.Logging

import scala.Numeric.Implicits._
import scala.reflect.ClassTag

/**
  * Dense Matrix
  */
class DenseMatrix[T: Numeric : ClassTag](override val _mat: Vector[DenseVector[T]])
  extends Mat[T](_mat) with Logging {

  def this(arr: Array[Array[T]]) = this(arr.map(new DenseVector(_)).toVector)

  def this(dvs: Seq[Seq[T]]) = this(dvs.map(new DenseVector(_)).toVector)

  /** Create a m*n matrix with zero values */
  def this(m: Int, n: Int) = this(Vector.fill(m)(DenseVector.zeros(n)))

  /** Return the number of columns */
  def colNum: Int = if (_mat.isEmpty) 0 else _mat(0).size

  /** Return the number of elements */
  def size: Int = rowNum * colNum

  /** Return the dimensions, i.e., (rowNum, colNum) */
  def dim: (Int, Int) = (rowNum, colNum)

  /** Return the mean of all the elements */
  def mean: Double = sum.toDouble() / this.size

  /** Return the ith column */
  def col(i: Int): DenseVector[T] = {
    require(i < colNum, s"col $i exceeds the colNum $colNum")
    val vs = _mat.map { r => r(i) }
    new DenseVector(vs)
  }

  /** Set M(i,j)=v */
  def update(i: Int, j: Int, v: T) {
    _mat(i)(j) = v
  }

  /** Update the `i`th row */
  def update(i: Int, dv: DenseVector[T]): Unit = {
    _mat(i).copy(dv)
  }

  /** Set the values of all elements to zero */
  def clear(): this.type = {
    (0 until _mat.size).foreach { i =>
      (0 until _mat(i).size).foreach { j =>
        _mat(i)(j) = zero
      }
    }
    this
  }

  /** Return the ith row */
  def row(i: Int): DenseVector[T] = {
    require(i < rowNum, s"row $i exceeds the rowNum ${rowNum}")
    _mat(i)
  }

  /** Return the `i`th row of this matrix */
  override def apply(i: Int): DenseVector[T] = row(i)

  /** Return the sum of each column */
  def colSum: DenseVector[T] = {
    val vs = Array.fill(colNum)(zero)
    _mat.foreach { r =>
      r.values.zipWithIndex.foreach {
        case (v, c) => vs(c) += v
      }
    }
    new DenseVector(vs)
  }

  /** Create a DenseMatrix by transposing this DenseMatrix */
  def transpose = new DenseMatrix(_mat.map(_.values).transpose)

  /** Test by a function `f` for all the elements */
  def forall(f: T => Boolean): Boolean = _mat.forall(_.forall(f))

  /** Apply a function to each element */
  def foreach(f: T => Unit) {
    _mat.foreach(_.foreach(f))
  }

  /** Apply a function to each row */
  def foreachRow(f: DenseVector[T] => Unit): Unit = {
    _mat.foreach(f)
  }

  /** Apply f to each active element */
  override def foreachActive(f: (Int, Int, T) => Unit): Unit =
    _mat.zipWithIndex.foreach {
      case (dv, i) => dv.foreachActive {
        case (j, v) => f(i, j, v)
      }
    }

  /** Create a new DenseMatrix by applying a function to each element */
  def map[V: Numeric : ClassTag](f: T => V): DenseMatrix[V] = new DenseMatrix(
    _mat.map(_.map(f)))

  /**
    * Create a new DenseMatrix by applying a function defined over the
    * value and indexes of each element
 *
    * @param f    f(v, rowIndex, colIndex) => V
    * @tparam V   return value type of f
    * @return  a new DenseMatrix with type V
    */
  def mapActive[V: Numeric : ClassTag](f: (Int, Int, T) => V): DenseMatrix[V] = {
    val mat: Vector[DenseVector[V]] = _mat.zipWithIndex.map {
      case (dv, i) =>
        val vs = dv.values.zipWithIndex.map {
          case (v, j) => f(i, j, v)
        }
        new DenseVector(vs)
    }
    new DenseMatrix(mat)
  }

  /** Apply a function to each Row */
  def mapRow[V](f: DenseVector[T] => V): Vector[V] = _mat.map(f)

  /** Create a new DenseMatrix whose value is the negative of this DenseMatrix */
  def unary_-(): DenseMatrix[T] = this.map(-_)

  /** Create a new matrix by adding x to each elements in this matrix */
  def +(x: T): DenseMatrix[T] = this.map(_ + x)

  /** Create a new matrix by minus x to each elements in this matrix */
  def -(x: T): DenseMatrix[T] = this.map(_ - x)

  /** Create a new matrix by multiplying x to each elements in this matrix */
  def *(x: T): DenseMatrix[T] = this.map(_ * x)

  /** Create a new matrix by dividing x to each elements in this matrix */
  def /(x: T): DenseMatrix[T] = {
    require(x != zero)
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        this.map(num.div(_, x))
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Add x to each elements in this matrix */
  def +=(x: T): this.type = selfOp(x)(_ + _)

  /** Subtract x to each elements in this matrix */
  def -=(x: T): this.type = selfOp(x)(_ - _)

  /** Multiply x to each elements in this matrix */
  def *=(x: T): this.type = selfOp(x)(_ * _)

  /** Divide x to each elements in this matrix */
  def /=(x: T): this.type = {
    require(x != zero)
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        selfOp(x)(num.div)
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }
  }

  /** Self-operation with a constant */
  private def selfOp(x: T)(op: (T, T) => T): this.type = {
    var i = 0
    while (i < _mat.size) {
      var j = 0
      while (j < _mat(i).size) {
        _mat(i)(j) = op(_mat(i)(j), x)
        j += 1
      }
      i += 1
    }
    this
  }

  /**
    * P-norm normalization of this Matrix
    *
    * @param p   p=1, sum-1;
    *            p=2, square-sum-1.
    * @return  A new Double DenseMatrix
    */
  def normalize(p: Int = 1): DenseMatrix[Double] = {
    val s = p match {
      case 1 => this.sum.toDouble()
      case 2 => this.norm
      case _ => throw new IllegalArgumentException(s"Unsupport $p norm normalization")
    }
    if (s == 0.0)
      throw new RuntimeException("normalization: sum is 0")
    this.map(_.toDouble / s)
  }

  /**
    * P-norm normalization of each row of this Matrix
    *
    * @param p   p=1, sum-1 normalization;
    *            p=2, square-sum-1 normalization
    * @return  A Double DenseMatrix
    */
  def normalizeRow(p: Int = 1): DenseMatrix[Double] =
    new DenseMatrix(this.mapRow(_.normalize(p)))

  /**
    * Dot each row with a DenseVector
 *
    * @return  a DenseVector with size equals to the rowNum of this Matrix
    */
  def dot(that: DenseVector[T]): DenseVector[T] = {
    require(colNum == that.size)
    val vec: Vector[T] = this.mapRow(_ dot that)
    new DenseVector(vec)
  }

  /**
    * Dot each row with a SparseVector
 *
    * @return  a DenseVector, size = this.rowNum
    */
  def dot(that: SparseVec[T]): DenseVector[T] = {
    val vec: Vector[T] = this.mapRow(_ dot that)
    new DenseVector(vec)
  }

  /** Element-wise equality comparison */
  def equal(that: DenseMatrix[T]): Boolean = {
    if (this.dim != that.dim) {
      false
    } else {
      (0 until rowNum).forall { i =>
        this.row(i) equal that.row(i)
      }
    }
  }

  /** Create a DenseMatrix by adding a DenseMatrix to this DenseMatrix */
  def +(that: DenseMatrix[T]): DenseMatrix[T] = matrixOp(that)(_ + _)

  /** Create a DenseMatrix by subtracting a DenseMatrix from this DenseMatrix */
  def -(that: DenseMatrix[T]): DenseMatrix[T] = matrixOp(that)(_ - _)

  /** Create a DenseMatrix by Element-wise production with another DenseMatrix */
  def *(that: DenseMatrix[T]): DenseMatrix[T] = matrixOp(that)(_ * _)

  /**
    * Create a DenseMatrix by element-wise dividing another DenseMatrix
 *
    * @note only avaible for Double or Float DenseMatrix
    * @return A new DenseMatrix
    */
  def /(that: DenseMatrix[T]): DenseMatrix[T] =
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        matrixOp(that)(num.div)
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }

  /** Element-wise matrix operation */
  private def matrixOp(that: DenseMatrix[T])(op: (T, T) => T): DenseMatrix[T] = {
    val arr = (0 until rowNum).map { i =>
      (0 until colNum).map { j =>
        op(this (i, j), that(i, j))
      }
    }
    new DenseMatrix(arr)
  }

  /** Add a DenseMatrix to this Matrix */
  def +=(that: DenseMatrix[T]): this.type = matrixSelfOp(that)(_ + _)

  /** Subtract a DenseMatrix from this Matrix */
  def -=(that: DenseMatrix[T]): this.type = matrixSelfOp(that)(_ - _)

  /** Element-wise product a DenseMatrix from this Matrix */
  def *=(that: DenseMatrix[T]): this.type = matrixSelfOp(that)(_ * _)

  /**
    * Element-wise divide a DenseMatrix
 *
    * @note `that` DenseMatrix should not contain zero values
    */
  def /=(that: DenseMatrix[T]): this.type =
    implicitly[Numeric[T]] match {
      case num: Fractional[T] =>
        matrixSelfOp(that)(num.div)
      case _ => throw new IllegalArgumentException("Undivisable numeric!")
    }

  /** Element-wise self-operation with another DenseMatrix */
  private def matrixSelfOp(that: DenseMatrix[T])(op: (T, T) => T): this.type = {
    for {
      i <- 0 until rowNum
      j <- 0 until colNum
    } _mat(i)(j) = op(_mat(i)(j), that(i, j))
    this
  }

  /**
    * Normal matrix production with a DenseMatrix is:
    * A(N,K) * B(K,M) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(k,j).
    *
    * However, since our matrix is stored in row-based, the above
    * computation requires to access each column of B, which might be
    * inefficient.
    * To save the computation, we take a Row-based Matrix product
    * manner,
    * A(N,K) * B(M,K) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(j,k), i.e., dot the ith row of A
    * and the jth row of B.
    *
    * @return  a DenseMatrix
    */
  def dot(that: DenseMatrix[T]): DenseMatrix[T] = {
    require(this.colNum == that.colNum,
      "matrix dot operation should with the same number of columns")
    val mat = _mat.map(_ dot that)
    new DenseMatrix(mat)
  }

  /** Create a new DenseMatrix by adding a SparseMatrix to this DenseMatrix */
  def +(that: SparseMat[T]): DenseMatrix[T] = {
    val mat = _mat.zipWithIndex.map {
      case (dv, i) => dv + that(i)
    }
    new DenseMatrix(mat)
  }

  /** Create a new DenseMatrix by substracting a SparseMatrix to this DenseMatrix */
  def -(that: SparseMat[T]): DenseMatrix[T] = {
    val mat = _mat.zipWithIndex.map {
      case (dv, i) => dv - that(i)
    }
    new DenseMatrix(mat)
  }

  /** Element-wise production with a SparseMatrix */
  def *(that: SparseMat[T]): SparseMat[T] =
    that.mapActive {
      case (i, j, v) => v * that(i, j)
    }

  /** Add a SparseMatrix to this DenseMatrix */
  def +=(that: SparseMat[T]): this.type = {
    (0 until rowNum).foreach { i =>
      this (i) += that(i)
    }
    this
  }

  /** Subtract a SparseMatrix to this DenseMatrix */
  def -=(that: SparseMat[T]): this.type = {
    (0 until rowNum).foreach { i =>
      this (i) -= that(i)
    }
    this
  }

  /**
    * Row-based Matrix dot product with a SparseMatrix:
    * A(N,K) * B(M,K) = C(N,M),
    * where C(i,j) = \sum_k A(i,k)B(j,k).
    *
    * @todo can be combined with doe DenseMatrix?
    * @return  a DenseMatrix
    */
  def dot(that: SparseMat[T]): DenseMatrix[T] = {
    val mat: Vector[DenseVector[T]] = this.mapRow(_ dot that)
    new DenseMatrix(mat)
  }

  /** Create a new DenseMatrix by clone this */
  override def clone(): DenseMatrix[T] = new DenseMatrix(_mat.map(_.clone()))

  /** Return the rows of this matrix in a Vector */
  def rows: Vector[DenseVector[T]] = _mat

  /** Convert to a tow-dimensional Array (Vector[Array[T]]) */
  def toVector: Vector[Array[T]] = _mat.map(_.values)

  /** Convert to a Double DenseMatrix */
  override def toDouble: DenseMatrix[Double] = this.map(_.toDouble)

  /** Convert this into a immutable SparseMatrix */
  def toImmutableSparse: immutable.SparseMatrix[T] =
    new immutable.SparseMatrix(
      this.mapRow(_.toImmutableSparse)
    )

  /** Convert this into a mutable SparseMatrix */
  def toMutableSparse: mutable.SparseMatrix[T] = new mutable.SparseMatrix(
    this.mapRow(_.toMutableSparse)
  )
}

object DenseMatrix extends MatLoader[DenseMatrix] {

  /** Create mxn DenseMatrix with values specified by `elem` */
  def fill[T: Numeric : ClassTag](m: Int, n: Int)(elem: => T) = new DenseMatrix(
    Vector.fill(m) {
      DenseVector.fill(n)(elem)
    })

  /** Create mxn DenseMatrix with all zeros */
  def zeros[T: Numeric : ClassTag](m: Int, n: Int): DenseMatrix[T] =
    DenseMatrix.fill(m, n)(implicitly[Numeric[T]].zero)

  /** Create mxn DenseMatrix with all ones */
  def ones[T: Numeric : ClassTag](m: Int, n: Int): DenseMatrix[T] =
    DenseMatrix.fill(m, n)(implicitly[Numeric[T]].one)

  /**
    * Parse a Double DenseMatrix from a string Iterator
 *
    * @param lines   each line with the format: "v,v,..."
    */
  def parse(lines: Iterator[String]): DenseMatrix[Double] = new DenseMatrix(
    lines.map(DenseVector.parse).toVector
  )

  /**
    * Parse a Float DenseMatrix from a string Iterator
 *
    * @param lines   each line with the format: "v,v,..."
    */
  def parseFloat(lines: Iterator[String]): DenseMatrix[Float] = new DenseMatrix(
    lines.map(DenseVector.parseFloat).toVector
  )

  /**
    * Parse a Int DenseMatrix from a string Iterator
 *
    * @param lines   each line with the format: "v,v,..."
    */
  def parseInt(lines: Iterator[String]): DenseMatrix[Int] = new DenseMatrix(
    lines.map(DenseVector.parseInt).toVector
  )

  /**
    * Parse a Long DenseMatrix from a string Iterator
 *
    * @param lines   each line with the format: "v,v,..."
    */
  override def parseLong(lines: Iterator[String]): DenseMatrix[Long] = new DenseMatrix(
    lines.map(DenseVector.parseLong).toVector
  )

  /** Create a Int random vector from uniform distribution [0, K) */
  def randInt(m: Int, n: Int, k: Int): DenseMatrix[Int] = new DenseMatrix(
    Vector.fill(m) {
      DenseVector.randInt(n, k)
    })

  /** Create a mxn random matrix using uniform distribution */
  def rand(m: Int, n: Int): DenseMatrix[Double] = new DenseMatrix(
    Vector.fill(m) {
      DenseVector.rand(n)
    })

  /** Create random matrix using Gauss(0, std) distribution */
  def randGauss(m: Int, n: Int, std: Double = 1.0): DenseMatrix[Double] =
    new DenseMatrix(Vector.fill(m) {
      DenseVector.randGaussian(n, std)
    })

  /** Create a Matrix from (i,j,v) triples */
  def fromTriples[T: Numeric : ClassTag](m: Int,
                                         n: Int,
                                         triples: TraversableOnce[(Int, Int, T)]): DenseMatrix[T] = {
    val dm = new DenseMatrix[T](m, n)
    triples.foreach {
      case (i, j, v) => dm(i, j) = v
    }
    dm
  }

  /**
    * Create a SparseMatrix from (rowID, colID, v) triples
    *
    * @param triples  (rowID, colID, value)
    * @tparam T  value type
    * @return  A immutable m*n SparseMatrix whose dimensions are determined by
    *          the maximum rowID and colID in the triples.
    */
  def fromTriples[T: Numeric : ClassTag](triples: Iterable[(Int, Int, T)]):
  DenseMatrix[T] = {
    val m = triples.map(_._1).max + 1
    val n = triples.map(_._2).max + 1
    fromTriples(m, n, triples)
  }
}
