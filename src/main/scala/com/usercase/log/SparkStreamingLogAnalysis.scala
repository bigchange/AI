package com.usercase.log

import com.bigchange.spark.Spark
import com.bigchange.util.{FileUtil, TimeUtil}
import org.json.JSONObject

import scala.util.{Failure, Success, Try}

/**
  * Created by C.J.YOU on 2016/12/27.
  * spark streaming queue log analysis
  */
object SparkStreamingLogAnalysis {

  val submissionTime= "Submission Time"

  val completionTime = "Completion Time"

  val properties = "Properties"

  val batchTime = "spark.streaming.internal.batchTime"

  val jobId = "Job ID"

  def parseJsonObject(string: String) = {

    Try({
      new JSONObject(string)
    }) match {
      case Success(r) => r
      case Failure(exception) => new JSONObject("{}")
    }
  }

  def  getJsonValue(json: JSONObject, key: String): Option[String] =  {

    Try({
      val value = json.get(key).toString
      Option(value)
    })   match {
      case Success(r) => Some(r.get)
      case Failure(ex) => Some("{}")
    }

  }

  def main(args: Array[String]) {

    val sc = Spark.apply("local","SparkStreamingLogAnalysis", 60).sc

    val data = sc.textFile("app-20161227113905-5603.inprogress").map{ x =>

      var ji = ""
      var st = ""
      var bt = ""
      var ct = ""

      val json = parseJsonObject(x)

      val jobid = getJsonValue(json, jobId).get

      if(jobid != "{}") {

        ji = jobid
        val stValue = getJsonValue(json, submissionTime).get

        if(stValue != "{}") {
             st = TimeUtil.formatTimeStamp(stValue, 0, 6)
        }

        val pro = getJsonValue(json, properties).get
        val proJson = parseJsonObject(pro)
        val btValue = getJsonValue(proJson, batchTime).get

        if(btValue != "{}") {
          bt = TimeUtil.formatTimeStamp(btValue, 0, 6)
        }

        val ctValue = getJsonValue(json, completionTime).get

        if(ctValue != "{}") {
          ct = TimeUtil.formatTimeStamp(ctValue, 0, 6)
         }

        (ji, (bt, st, ct))

      }
      else ("",("","",""))

    }.filter(_._1 != "").reduceByKey { (x,y) =>
      if(x._1 == "") {
        (y._1, y._2, x._3)
      } else  {
        (x._1, x._2, y._3)
      }
    }.map(x => (x._1, x._2._1 + "\t" + x._2._2 + "\t" + x._2._3)).sortBy(_._1.toInt,ascending = true).map(_._2)

    data.foreach(println)

    FileUtil.normalFileWriter(path = "app-20161227113905-5603.result", data.collect(), isAppend = true)


  }

}
