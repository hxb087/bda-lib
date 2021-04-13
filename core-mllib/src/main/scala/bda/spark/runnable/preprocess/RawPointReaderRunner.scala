package bda.spark.runnable.preprocess

import bda.common.obj.RawPoint
import bda.common.util.MapUtil
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
 * Reader of RawPoint.
 */
object RawPointReaderRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RawPointReaderRunner") {
      head("RawPointReader")
      opt[String]("input_pt")
        .required()
        .text("path of the file which is stored in nonstandard format")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("path of the file which is stored in RawPoint format")
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
          |For example, the following command runs this app to preprocess for RawPoint reading:
          |
          | spark-submit --class bda.spark.runnable.preprocess.RawPointReaderRunner out/artifacts/*/*.jar \
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
      .setAppName(s"Spark Preprocess of RawPointReader")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val f = sc.textFile(p.input_pt).map(parse(_, p.has_id, p.has_label, p.seperator))

    f.saveAsTextFile(p.output_pt)
  }

  /**
   * Parse the file which is in nonstandard format to RawPoint data structure.
   *
   * @param s Raw string.
   * @param has_id Whether has an id field.
   * @param has_label Whether has a label field.
   * @param separator String used to seperate fields.
   */
  def parse(s: String,
            has_id: Boolean = false,
            has_label: Boolean = false,
            separator: String = "\t"): RawPoint = {

    (has_id, has_label) match {

      case (true, true) =>
        val Array(id, label, fvs) = s.split(separator, 3)
        val fv = MapUtil.parse(fvs).map {
          case (k, v) => (k, v.toDouble)
        }
        new RawPoint(id, label.toDouble, fv)

      case (true, false) =>
        val Array(id, fvs) = s.split(separator, 2)
        val label = RawPoint.default_label
        val fv = MapUtil.parse(fvs).map {
          case (k, v) => (k, v.toDouble)
        }
        new RawPoint(id, label, fv)

      case (false, true) =>
        val Array(label, fvs) = s.split(separator, 2)
        val id = RawPoint.default_id
        val fv = MapUtil.parse(fvs).map {
          case (k, v) => (k, v.toDouble)
        }
        new RawPoint(id, label.toDouble, fv)

      case (false, false) =>
        val id = RawPoint.default_id
        val label = RawPoint.default_label
        val fv = MapUtil.parse(s).map {
          case (k, v) => (k, v.toDouble)
        }
        new RawPoint(id, label, fv)
    }
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    has_id: Boolean = true,
                    has_label: Boolean = true,
                    seperator: String = "\t")
}