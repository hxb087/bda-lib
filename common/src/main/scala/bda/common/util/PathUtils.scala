package bda.common.util

import java.io.File

/**
 * Created by ljf on 2020/4/24.
 * 管理系统中涉及到的所有的存储路径
 */
object PathUtils {


  final val VISUALIZATION_PATH = "visualization"

  def getVisualPath(rootPath: String, paths: String*): String = {
    getPath(rootPath + File.separator + VISUALIZATION_PATH, paths: _*)
  }

  def getPath(rootPath: String, paths: String*): String = {
    var finalPath = rootPath
    for (path <- paths) {
      finalPath = finalPath + File.separator + path;
    }
    finalPath
  }


}
