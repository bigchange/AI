package com.bigchange.log

import org.apache.log4j.{Logger, PropertyConfigurator}

/**
  * Created by C.J.YOU on 2016/1/15.
  * 打log日志的类需要继承此trait
  * spark 有实现对应的log ->  org.apache.spark.internal.Logging
  */
trait CLogger extends Serializable {


  // PropertyConfigurator.configure("/home/telecom/conf/log4j.properties")

  val loggerName = this.getClass.getName

  lazy val logger = Logger.getLogger(loggerName)

  def logConfigure(path: String) = PropertyConfigurator.configure(path)

  def debug(msg: String) = logger.debug(msg)

  def info(msg: String) = logger.info(msg)

  def warn(msg: String) = logger.warn(msg + "<<<<==============")

  def error(msg: String) = logger.error(msg)

  def exception(e: Exception) = logger.error(e.getStackTrace)

  /**
    * 自定义输出的日志格式
    * @param msg 消息
    */
  def warnLog(msg: String) {

    val info = logFileInfo
    logger.warn("{}[{}]:{}", info._1, info._2, msg)

  }

  def errorLog(msg: String) {

    val info = logFileInfo
    logger.error("{}[{}]:{}", info._1, info._2, msg)

  }

  /**
    * 获取日志所在的文件信息
    * @return (文件名， 位置)
    */
  def logFileInfo: (String, String) = (Thread.currentThread.getStackTrace()(2).getFileName, Thread.currentThread.getStackTrace()(2).getLineNumber.toString)


}
