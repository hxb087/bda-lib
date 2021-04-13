package bda.common.util

/**
  * Created by Administrator on 2020/3/12.
  * 应用提交工具类
  */
object AppUtils {

  /**
    *
    * @param name
    * @return
    *         name 后面增加时间戳
    */
  def AppName(name: String): String=name.concat("-").concat(System.currentTimeMillis()+"")


  def main(args: Array[String]) {
    println(AppName("test"))
  }
}
