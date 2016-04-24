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
   def createFile(path:String): Unit ={
    val file = new File(path)
    if(!isExist(path)){
      file.createNewFile()
    }
  }
  def writeToFile(path: String, array:Array[String]): Unit = {
    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)
    for (arr <- array){
      writer.append(arr + "\n")
    }
    writer.flush()
    writer.close()
  }
}
