package bda.spark.runnable.preprocess

import bda.common.linalg.immutable.SparseVector
import bda.common.obj.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
 * Reader of LabeledPoint.
 */
object LabeledPointReaderRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("LabeledPointReaderRunner") {
      head("LabeledPointReader")
      opt[String]("input_pt")
        .required()
        .text("path of the file which is stored in nonstandard format")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("path of the file which is stored in LabeledPoint format")
        .action((x, c) => c.copy(output_pt = x))
      opt[Boolean]("has_id")
        .required()
        .text("whether has an id field")
        .action((x, c) => c.copy(has_id = x))
      opt[Boolean]("has_label")
        .required()
        .text("whether has a label field")
        .action((x, c) => c.copy(has_label = x))
      opt[String]("seperator")
        .required()
        .text("string used to seperate fields")
        .action((x, c) => c.copy(seperator = x))
      note(
        """
          |For example, the following command runs this app to preprocess for LabeledPoint reading:
          |
          | spark-submit --class bda.spark.runnable.preprocess.LabeledPointReaderRunner out/artifacts/*/*.jar \
          |   --input_pt ... \
          |   --output_pt ... \
          |   --has_id true \
          |   --has_label true \
          |   --seperator "\t"
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(p: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of LabeledPointReader")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val f = sc.textFile(p.input_pt).map(parse(_, p.has_id, p.has_label, p.seperator))

    f.saveAsTextFile(p.output_pt)
  }

  /**
   * Parse the file which is in nonstandard format to LabeledPoint data structure.
   *
   * @param s Raw string.
   * @param has_id Whether has an id field.
   * @param has_label Whether has a label field.
   * @param separator String used to seperate fields.
   */
  def parse(s: String,
            has_id: Boolean = true,
            has_label: Boolean = true,
            separator: String = "\t"): LabeledPoint = {

    (has_id, has_label) match {

      case (true, true) =>
        val Array(id, label, fvs) = s.split(separator, 3)
        val fv = SparseVector.parse(fvs)
        new LabeledPoint(id, label.toDouble, fv)

      case (true, false) =>
        val Array(id, fvs) = s.split(separator, 2)
        val label = LabeledPoint.default_label
        val fv = SparseVector.parse(fvs)
        new LabeledPoint(id, label, fv)

      case (false, true) =>
        println(s)

        val Array(label, fvs) = s.split(separator, 2)
        val id = LabeledPoint.default_id
        val fv = SparseVector.parse(fvs)
        new LabeledPoint(id, label.toDouble, fv)

      case (false, false) =>
        val id = LabeledPoint.default_id
        val label = LabeledPoint.default_label
        val fv = SparseVector.parse(s)
        new LabeledPoint(id, label, fv)
    }
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    has_id: Boolean = true,
                    has_label: Boolean = true,
                    seperator: String = "\t")
}