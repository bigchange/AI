package com.bigchange.log

import org.apache.log4j.{PropertyConfigurator, Logger}

/**
  * Created by C.J.YOU on 2016/1/15.
  */
object SUELogger {


  // PropertyConfigurator.configure("/home/telecom/conf/log4j.properties")

  private  val logger = Logger.getLogger("SUELogger")

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

}
