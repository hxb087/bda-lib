package com.golaxy.bda.utils

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext

/**
 * Created by Administrator on 2020/1/16.
 */
object HDFSUtils {


  /**
   * 获得HDFS文件系统
   *
   * @param sc       SparkContext 上下文
   * @param filePath 文件路径
   * @return
   */
  private def getFileSystem(sc: SparkContext, filePath: String): FileSystem = {
    new Path(filePath).getFileSystem(new Configuration())
  }


  /**
   * 判断文件是否存在
   *
   * @param sc
   * @param filePath
   * @return
   */
  def existsFile(sc: SparkContext, filePath: String): Boolean = {
    getFileSystem(sc, filePath).exists(new Path(filePath))
  }


  /**
   *
   * @param sc
   * @param filePath
   * @return
   */
  def listDirPath(sc: SparkContext, filePath: String): Array[String] = {
    val fs = getFileSystem(sc, filePath)
    val fileStatus: Array[FileStatus] = fs.listStatus(new Path(filePath))
    fileStatus.map(
      status => {
        status.getPath.toString
      }
    )
  }


  /**
   * 列出目录下所有文件或目录名
   *
   * @param sc
   * @param filePath
   * @return
   */
  def listDirNames(sc: SparkContext, filePath: String): Array[String] = {
    val fs = getFileSystem(sc, filePath)
    val fileStatus: Array[FileStatus] = fs.listStatus(new Path(filePath))
    fileStatus.map(
      status => {
        status.getPath.getName
      }
    )
  }


  /**
   * 删除HDFS目录
   *
   * @param filePath
   * @param recursive 是否递归子目录
   * @return
   */
  def deleteHDFSPath(filePath: String, recursive: Boolean): Boolean = {
    var flag: Boolean = true
    val path = new Path(filePath)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (fs.exists(path)) {
      flag = fs.delete(path, recursive)
    }
    flag
  }

  /**
   * 创建HDFS路径，如果存在就不创建
   *
   * @param filePath
   * @return
   */
  def makeHDFSDirs(filePath: String): Boolean = {
    var flag: Boolean = true
    val path = new Path(filePath)
    val fs = FileSystem.get(path.toUri, new Configuration())
    if (!fs.exists(path)) {
      flag = fs.mkdirs(path)
    }
    flag
  }

  /**
   * 文件(夹)拷贝
   *
   * @param src
   * @param dest
   * @param deleteSource
   * @return
   */
  def copy(src: String, dest: String, deleteSource: Boolean = false): Boolean = {
    var flag: Boolean = true
    val scrPath = new Path(src)
    val fs = FileSystem.get(scrPath.toUri, new Configuration())
    if (fs.exists(scrPath)) {
      flag = FileUtil.copy(fs, scrPath, fs, new Path(dest), deleteSource, new Configuration())
    }
    flag
  }

  def rename(src: String, dest: String): Boolean = {
    var flag: Boolean = true
    val scrPath = new Path(src)
    val fs = FileSystem.get(scrPath.toUri, new Configuration())
    if (fs.exists(scrPath)) {
      flag = fs.rename(scrPath, new Path(dest))
    } else {
      throw new FileNotFoundException(s" src path: ${scrPath} not found")
    }
    flag
  }

}
