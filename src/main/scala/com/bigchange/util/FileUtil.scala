package com.bigchange.util

import java.io._


/**
  * Created by C.J.YOU on 2016/1/14.
  * File 操作的工具类
  */
object FileUtil {

  private def isExist(path:String): Boolean ={
    val file = new File(path)
    file.exists()
  }

  /** 创建目录 */
  def mkDir(name: String): Unit = {
    val dir = new File(name)
    if(!isExist(name)){
      dir.mkdir
    }
  }

  /**
    * create file
    * @param path file path
    */
  def createFile(path:String): Unit ={
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }

  /**
    * write to file
    * @param path file path
    * @param array data
    * @param isAppend 文件追加与否  if true, then bytes will be written
    *                   to the end of the file rather than the beginning
    */
  def writeToFile(path: String, array:Array[String], isAppend:Boolean=false): Unit = {

    val out = new FileOutputStream(new File(path),isAppend)
    val writer = new PrintWriter(out, false)

    for (arr <- array) {
      writer.append(arr + "\n")
    }

    writer.flush()
    writer.close()

  }

  /**
    * 写入成CSV格式的文件
    * @param path file name
    * @param header header note
    * @param array data array
    */
  def writeToCSV(path: String, header:Array[String], array:Array[String]): Unit = {

    createFile(path)

    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    // save head info
    for(index <- header.indices) {

      if(index + 1 == header.length)
        writer.append(header(index) + "\n")
      else
        writer.append( header(index) + ",")
    }
    writer.flush()
    writer.close()
    // save data info
    writeToFile(path, array)

  }

}
