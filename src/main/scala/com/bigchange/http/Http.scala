package com.bigchange.http

import com.bigchange.log.CLogger
import dispatch.Defaults._
import dispatch._

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Created by C.J.YOU on 16/1/13.
  */
abstract class Http  extends  CLogger {

  /**
    * 格式化成http请求的参数
    * @param parameter 参数HashMap
    * @return 参数
    */
  def getParameters(parameter: mutable.HashMap[String,String]): String = {

    var strParam:String = ""
    val iterator = parameter.keySet.iterator

    while(iterator.hasNext) {

      val key = iterator.next()

      if(parameter.get(key) != null) {

        strParam += key + "=" + parameter.get(key).get

        if(iterator.hasNext) strParam += "&"

      }
    }

    strParam

  }

  /**
    * 得到最终的url
    * @param url 一级url域名
    * @param parameters 参数内容map
    * @return url
    */
  def getUrl(url:String, parameters:mutable.HashMap[String,String]): String = {

    val strParam = getParameters(parameters)
    var strUrl = url

    if (strParam != null) {

      if (url.indexOf("?") >= 0)
        strUrl += "&" + strParam
      else
        strUrl += "?" + strParam

    }

    strUrl
  }

  /**
    * get 请求
    * @param strUrl  一级url域名
    * @param parameters 参数内容 map
    * @param parse 暂无实际含义
    */
  def get(strUrl:String, parameters:mutable.HashMap[String,String], parse: String): Unit = {

    try {

      val finalUrl = getUrl(strUrl, parameters)
      val req = url(finalUrl)
      val response = Http(req OK as.String)

      response onComplete {
        case Success(content) =>
         // println("get Success content:" + content)
        case Failure(t) =>
        // println("get Success content:"+ t.getMessage)
      }

    } catch {
      case e:Exception =>
        error("get url request Exception")
    }
  }

  /**
    * post 请求
    * @param strUrl 一级url域名
    * @param parameters 参数内容 map
    * @param parse 暂无实际含义
    */
  def post(strUrl:String, parameters:mutable.HashMap[String,String], parse: String): Unit = {

    val post = url(strUrl) << parameters
    val response : Future[String] = Http(post OK as.String)

    response onComplete {
      case Success(content) =>
        // parse(content)
        println("post Success content:"+content)
      case Failure(t) =>
        println("post Failure content:"+t)
    }
  }
}
