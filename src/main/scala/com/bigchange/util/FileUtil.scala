package com.bigchange.util

import java.io._


/**
  * Created by C.J.YOU on 2016/1/14.
  * 具体实现如何写文件的操作类
  */
object FileUtil extends FileContentWriter{

  /**
    * 写入成CSV格式的文件
    * @param path file name
    * @param header header note
    * @param array data array
    */
  def writeToCSV(path: String, header:Array[String], array:Array[((String, String),Int)]): Unit = {

    val out = new FileOutputStream(new File(path),true)
    val writer = new PrintWriter(out, false)

    // save head info
    for(index <- header.indices) {

      if(index + 1 == header.length)
        writer.append(header(index) + "\n")
      else
        writer.append(header(index) + ",")
    }
    writer.flush()

    for (arr <- array) {
      writer.append(arr._1._1 + "," + arr._1._2 + "," + arr._2 + "\n")
    }
    writer.flush()
    writer.close()

  }

  /**
    * 指定格式的数据写入成CSV格式的文件
    * @param path file name
    * @param header header note
    * @param array data array
    */
  def writeToCSVCmp(path: String, header:Array[String], array:Array[((String, String),(Int, Int, Int))]): Unit = {

    val out = new FileOutputStream(new File(path),true)

    val writer = new PrintWriter(out, false)

    // save head info
    for(index <- header.indices) {

      if(index + 1 == header.length)
        writer.append(header(index) + "\n")
      else
        writer.append(header(index) + ",")
    }
    writer.flush()

    for (arr <- array) {
      writer.append(arr._1._1 + "," + arr._1._2 + "," + arr._2._1 + "," + arr._2._2 + "," + arr._2._3 + "\n")
    }
    writer.flush()
    writer.close()

  }

  /**
    * rootDir下面按天按小时生成文件
    * @param rootDir  上级目录路径
    * @param data 数据内容
    */
  def saveData(rootDir:String, data:Array[String]):Unit = {

    val dataDir = rootDir + "/" + TimeUtil.getDay
    val dataFile = dataDir + "/" + TimeUtil.getCurrentHour

    mkDir(dataDir )
    createFile(dataFile)
    normalFileWriter(dataFile, data, isAppend = true)

  }

}
