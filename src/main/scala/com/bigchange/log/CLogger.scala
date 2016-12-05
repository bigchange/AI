package com.bigchange.log

import org.apache.log4j.Logger

/**
  * Created by C.J.YOU on 2016/1/15.
  */
object CLogger {


  // PropertyConfigurator.configure("/home/telecom/conf/log4j.properties")

  private  val logger = Logger.getLogger("CLogger")

  def debug(msg: String): Unit = {
    logger.debug(msg)
  }

  def info(msg: String): Unit = {
    logger.info(msg)
  }

  def warn(msg: String): Unit = {
    logger.warn(msg + "<<<<==============")
  }

  def error(msg: String): Unit = {
    logger.error(msg)
  }

  def exception(e: Exception): Unit = {
    logger.error(e.getStackTrace)
  }

  /**
    * 自定义输出的日志格式
    * @param info 标识
    * @param msg 消息
    */
  def warnLog(
               info: (String, String),
               msg: String) {

    logger.warn("{}[{}]：{}", info._1, info._2, msg)
  }

  def errorLog(
                info: (String, String),
                msg: String) {

    logger.error("{}[{}]：{}", info._1, info._2, msg)
  }

  /**
    * 获取日志所在的文件信息
    * @return
    */
  def fileInfo: (String, String) = (Thread.currentThread.getStackTrace()(2).getFileName, Thread.currentThread.getStackTrace()(2).getLineNumber.toString)


}
