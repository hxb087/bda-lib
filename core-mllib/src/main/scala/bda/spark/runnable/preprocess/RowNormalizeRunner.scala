package bda.spark.runnable.preprocess

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}

/**
 * Row Normalizer.
 */
object RowNormalizeRunner {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("aka").setLevel(Level.WARN)

    val default_params = Params()

    val parser = new OptionParser[Params]("RowNormalizeRunner") {
      head("RowNormalize")
      opt[String]("input_pt")
        .required()
        .text("path of the file which is stored in nonstandard format")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("output_pt")
        .required()
        .text("path of the file which is stored in standard format")
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
          |For example, the following command runs this app to preprocess for row normalize:
          |
          | spark-submit --class bda.spark.runnable.preprocess.RowNormalizeRunner out/artifacts/*/*.jar \
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
      .setAppName(s"Spark Preprocess of RowNormalize")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val f_input = sc.textFile(p.input_pt)

    val default_label = Double.NaN

    val f_output = p.has_id match {

      case true =>
        f_input.map { ln =>
          p.has_label match {
            case true =>
              val Array(id, label, value) = ln.split(p.seperator, 3)
              s"$id\t$label\t$value"
            case false =>
              val Array(id, value) = ln.split(p.seperator, 2)
              s"$id\t$default_label\t$value"
          }
        }

      case false =>
        f_input.zipWithIndex().map{ ln =>
          val id = ln._2.toString
          val lv = ln._1
          p.has_label match {
            case true =>
              val Array(label, value) = lv.split(p.seperator, 2)
              s"$id\t$label\t$value"
            case false =>
              s"$id\t$default_label\t$lv"
          }
        }
    }

    f_output.saveAsTextFile(p.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    output_pt: String = "",
                    has_id: Boolean = true,
                    has_label: Boolean = true,
                    seperator: String = "\t")
}