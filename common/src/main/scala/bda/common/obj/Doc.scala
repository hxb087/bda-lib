package bda.common.obj

/**
  * Document with string words
 *
  * @param id  docId, which can be used to store side information of a document
  * @param label label of document
  * @param words  word sequence
  */
case class RawDoc(id: String,
                  label: Double,
                  words: Array[String]) {

  def this(id: String, label: Double, words: Seq[String]) = this(id, label, words.toArray)

  /** Number of words in the document */
  def size = words.length

  /** Unigram (word) sequence of this document */
  def unigrams: Array[String] = words

  /**
    * Convert to a Doc represented by word ids
 *
    * @param w2id  word-id map. Words not in the map will be filtered.
    * @return A [[bda.common.obj.Doc]] instance.
    */
  def toDoc(w2id: Map[String, Int]): Doc = {
    val wids = words.filter(w => w2id.contains(w)).map(w => w2id(w))
    new Doc(id, label, wids)
  }

  /**
    * Generate a string representation of RawDoc.
    * if the doc has id, format is: "did    word word ..."
    * if without a doc id, format is: "word word ..."
    */
  override def toString: String = s"$id\t$label\t" + words.mkString(" ")
}

object RawDoc {
  val default_id = ""

  val default_label = Double.NaN

  /**
    * Parse a RawDoc from a string
 *
    * @param s  if the doc has id, format is: "id  label  word word ..."
    *           if without a doc id, format is: "label  word word ..."
    */
  def parse(s: String): RawDoc = s.split("\t", 3) match {
    case Array(id, label, ws) =>
      new RawDoc(id, label.toDouble, ws.split(" "))

    case Array(id, ws) =>
      val label = RawDoc.default_label
      new RawDoc(id, label, ws.split(" "))

    case Array(ws) =>
      val id = RawDoc.default_id
      val label = RawDoc.default_label
      new RawDoc(id, label, ws.split(" "))
    case other => throw new IllegalArgumentException(s"parse RawDoc failed: ${s}")
  }

  /** If did is not provided, random generate a UUID. */
  def apply(words: Array[String]): RawDoc = new RawDoc(default_id, default_label, words)

  /** If did is not provided, random generate a UUID. */
  def apply(words: Seq[String]): RawDoc = apply(words.toArray)
}

/**
  * General documents without specifying the word type
  *
  * @constructor Create a new document with a word sequence
  * @param id  docId
  * @param label label of document
  * @param words   word sequence
  */
case class Doc(id: String,
               label: Double,
               words: Array[Int]) {

  def this(id: String, label: Double, words: Seq[Int]) = this(id, label, words.toArray)

  /** Number of words in the document */
  def size: Int = words.length

  /** Return the unigram (word) sequence of this document */
  def unigrams: Array[Int] = words

  /** Return the bigram sequence of this document */
  def bigrams: Array[(Int, Int)] = if (words.length < 2)
    Array[(Int, Int)]()
  else
    words.sliding(2).map {
      case Array(wi, wj) => (wi, wj)
    }.toArray

  /**
    * Return the biterm sequence of this document
 *
    * @param win  context size
    * @return biterm sequence in the  document
    */
  def biterms(win: Int = 15): Array[(Int, Int)] =
    (0 until words.size - 1).flatMap { i =>
      (i + 1 until math.min(i + win, words.size)).map { j =>
        if (words(i) < words(j))
          (words(i), words(j))
        else
          (words(j), words(i))
      }
    }.toArray

  /**
    * Convert into a RawDoc that with string words
 *
    * @param id2w  WordId-String map. Words not in the map will be filtered.
    * @return A [[bda.common.obj.RawDoc]] instance
    */
  def toRawDoc(id2w: Map[Int, String]): RawDoc = {
    val ws = words.filter(w => id2w.contains(w)).map {
      id2w(_)
    }
    new RawDoc(id, label, ws)
  }

  /** Format this document to a string, format: "word word ..." */
  override def toString: String = {
    val s = words.map(_.toString).mkString(" ")
    s"$id\t$label\t$s"
  }
}

object Doc {

  val default_id = ""

  val default_label = Double.NaN

  def apply(words: Seq[Int]) = new Doc(default_id, default_label, words.toArray)

  def apply(words: Array[Int]) = new Doc(default_id, default_label, words)

  /**
    * Parse a document from a string
 *
    * @param s  format: "word word ..."
    * @return A document with word type to be string
    */
  def parse(s: String): Doc = s.split("\t", 3) match {
    case Array(id, label, ws) =>
      new Doc(id, label.toDouble, ws.split(" ").map(_.toInt))
    case Array(id, ws) =>
      val label = Doc.default_label
      new Doc(id, label, ws.split(" ").map(_.toInt))
    case Array(ws) =>
      val id = Doc.default_id
      val label = Doc.default_label
      Doc(id, label, ws.split(" ").map(_.toInt))
    case other => throw new IllegalArgumentException(s"parse Doc failed: ${s}")
  }
}