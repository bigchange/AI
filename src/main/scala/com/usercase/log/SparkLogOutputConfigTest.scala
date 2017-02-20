package com.usercase.log

import com.bigchange.log.CLogger
import com.bigchange.spark.Spark

import scala.util.{Failure, Success, Try}

/**
  * Created by C.J.YOU on 2016/12/15.
  */
object SparkLogOutputConfigTest extends CLogger{

  val sc = Spark.apply("local","SparkLogTest", 60).sc

  def main(args: Array[String]) {

    val Array(path,logFile) = args

    logConfigure(logFile)

    val data = Try(sc.textFile(path)) match {

      case Success(r) =>
        info("count:" + r.count())
      case Failure(exception) =>
        error("this is test error log")
        warn("this is warning log")
        info("this is info log")
        debug("this is debug log")
        errorLog("this is errorLog")
    }




  }

}
