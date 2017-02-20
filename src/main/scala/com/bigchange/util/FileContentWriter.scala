package com.bigchange.util

import java.io.{File, FileOutputStream, PrintWriter}

/**
  * Created by C.J.YOU on 2017/1/12.
  * File 操作的工具类， 封装成class便于重写其方法和复用
  */
class FileContentWriter {

  private def isExist(path:String) = new File(path).exists()

  /** 创建目录 */
  def mkDir(name: String): Unit = {

    val dir = new File(name)

    if(!isExist(name)) dir.mkdir

  }

  /**
    * create file
    * @param path file path
    */
  def createFile(path:String): Unit = {

    val file = new File(path)

    if(!isExist(path))  file.createNewFile()

  }

  /**
    * write to file
    * @param path file path
    * @param array data
    * @param isAppend 文件追加与否  if true, then bytes will be written
    *                   to the end of the file rather than the beginning
    */
  def normalFileWriter(path: String, array:Array[String], isAppend:Boolean=false): Unit = {

    val out = new FileOutputStream(new File(path),isAppend)
    val writer = new PrintWriter(out, false)

    for (arr <- array) {
      writer.append(arr + "\n")
    }

    writer.flush()
    writer.close()

  }

}

object FileContentWriter {

  private var fcw: FileContentWriter = _

  def apply: FileContentWriter = {
    if (fcw == null) fcw = new FileContentWriter
    fcw
  }

  def getInstance = fcw

}

