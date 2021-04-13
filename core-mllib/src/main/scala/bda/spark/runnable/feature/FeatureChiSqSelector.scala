package bda.spark.runnable.feature

/**
 * @author ：huxb
 * @date ：2020/11/17 11:25
 * @description：TODO
 * @modified By：
 * @version: $ 1.0
 */

import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import org.apache.spark.mllib.linalg.Vectors

/**
 * 卡方选择
 * input_pt:输入文件
 * featureNum:选择列个数
 * output_pt:输出文件
 * outModel_pt:输出模型
 * appname:名称
 */
object FeatureChiSqSelector {
  /** command line parameters */
  case class Params(input_pt: String = "",
                    featureNum: Int = 1,
                    output_pt: String = "",
                    outModel_pt: String = ""
                   )
  def main(args: Array[String]) {
    val default_params = Params()
    val parser = new OptionParser[Params]("StringIndex") {
      head("StringIndex")
      opt[String]("input_pt")
        .required()
        .text("Input document file path")
        .action((x, c) => c.copy(input_pt = x))
      opt[Int]("featureNum")
        .required()
        .text("test Input document file path")
        .action((x, c) => c.copy(featureNum = x))
      opt[String]("output_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(output_pt = x))
      opt[String]("outModel_pt")
        .required()
        .text("Output document file path")
        .action((x, c) => c.copy(outModel_pt = x))
    }
    parser.parse(args, default_params).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
    def run(p: Params): Unit = {

      val conf = new SparkConf()
        .setAppName("FeatureChiSqSelector")
//        .setMaster("local")
      val sc = new SparkContext(conf)
      val data = MLUtils.loadLibSVMFile(sc, p.input_pt)
      val discretizedData = data.map { lp =>
        LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map { x => (x / 16).floor } ) )
      }
      val selector = new ChiSqSelector(p.featureNum)
      val transformer = selector.fit(discretizedData)
      transformer.save(sc,p.outModel_pt)
      val filteredData = discretizedData.map { lp =>
        LabeledPoint(lp.label, transformer.transform(lp.features))
      }
      MLUtils.saveAsLibSVMFile(filteredData,p.output_pt)

    }
  }
}
