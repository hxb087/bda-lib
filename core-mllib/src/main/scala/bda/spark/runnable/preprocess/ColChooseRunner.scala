package bda.spark.runnable.preprocess

import org.apache.commons.lang3.StringEscapeUtils
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser

/**
 * Preprocess for ColChoose.
 */
object ColChooseRunner {

  def main(args: Array[String]) {
    val default_params = Params()

    val parser = new OptionParser[Params]("RunSparkPreprocessColChoose") {
      head("RunSparkPreprocessColChoose: an example app of preprocessing for ColChoose.")
      opt[String]("input_pt")
        .required()
        .text("directory of the input data file")
        .action((x, c) => c.copy(input_pt = x))
      opt[String]("columns")
        .required()
        .text("the specified columns")
        .action((x, c) => c.copy(columns = x))
      opt[String]("output_pt")
        .required()
        .text("directory of the output data file")
        .action((x, c) => c.copy(output_pt = x))
      opt[String]("seperator")
        .required()
        .text("separator to split rows")
        .action((x, c) => c.copy(seperator = x))
      note(
        """
          |For example, the following command runs this app to preprocess for ColChoose:
          |
          | java -jar out/artifacts/*/*.jar \
          |   --input_pt ... \
          |   --columns 1,2,3 \
          |   --output_pt ... \
          |   --separator ...
        """.stripMargin)
    }

    parser.parse(args, default_params) match {
      case Some(params) => run(params)
      case None => System.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf()
      .setAppName(s"Spark Preprocess of Columns Choose")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val cols = params.columns.split(",").map(_.toInt - 1)
    val sep: String = StringEscapeUtils.unescapeJava(params.seperator)

    sc.textFile(params.input_pt).map { ln =>
      val row = ln.split(params.seperator)
      cols.map(i => row(i)).mkString(sep)
    }.saveAsTextFile(params.output_pt)
  }

  /** command line parameters */
  case class Params(input_pt: String = "",
                    columns: String = "",
                    output_pt: String = "",
                    seperator: String = ",")
}