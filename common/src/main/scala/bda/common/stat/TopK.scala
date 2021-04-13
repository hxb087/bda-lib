package bda.common.stat

import scala.collection.mutable.ListBuffer


/**
  * A collection keep the K highest elements in order
  */
class TopK[T](val K: Int)(implicit val ord: Ordering[T]) {
  // list ordered in increasing order
  private val list = new ListBuffer[T]()
  /** The number of elements in the collection */
  var size: Int = 0

  def min: T = list.head

  def max: T = list.last

  /** Add a element into the topK collection */
  def add(e: T): Unit = {
    // buffer is full
    if (size >= K) {
      if (ord.lt(e, list.head))
        return
      else {
        list.remove(0)
        size -= 1
      }

    }
    insert(e)
  }

  /** Insert e into the list, and keep the order */
  private def insert(e: T): Unit = {
    // find the last index of the element less than e
    var i = 0
    val iter = list.iterator
    while (i < size && ord.lt(iter.next(), e)) {
      i += 1
    }
    list.insert(i, e)
    size += 1
  }

  /** Convert the K elements into a size-K array in decreasing order */
  def sorted: List[T] = list.toList.reverse

  def toList: List[T] = list.toList
}

object TopK {

  /**
    * Extract the topK largest elements in a TraversableOnce instance
    */
  def apply[T](vs: TraversableOnce[T],
               K: Int)(implicit ord: Ordering[T]): TopK[T] = {
    val topK = new TopK[T](K)(ord)
    vs.foreach { v =>
      topK.add(v)
    }
    topK
  }
}