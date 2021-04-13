package bda.common

import bda.common.util.io.readResource

/**
  * Some commonly used small datasets
  */
package object data {
  /** Chinese stop word set */
  lazy val chStopWords: Set[String] = readResource("/stopWordsCN.txt").toSet

  /** English stop word set */
  lazy val enStopWords: Set[String] = readResource("/stopWordsEN.txt").toSet

  /** Chinese and English stop word set */
  lazy val allStopWords: Set[String] = chStopWords ++ enStopWords
}
