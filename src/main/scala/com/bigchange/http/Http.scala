package com.bigchange.http

import dispatch.Defaults._
import dispatch._

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Created by C.J.YOU on 16/1/13.
  */
abstract class Http {

  def getParameters(parameter: mutable.HashMap[String,String]): String = {
    var strParam:String = ""
    val iterator = parameter.keySet.iterator
    while(iterator.hasNext) {
      val key = iterator.next()
      if(parameter.get(key) != null){
        strParam += key + "=" + parameter.get(key).get
        if(iterator.hasNext)
          strParam += "&"
      }
    }

    strParam
  }

  def getUrl(url:String, parameters:mutable.HashMap[String,String]): String ={
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

  def get(strUrl:String, parameters:mutable.HashMap[String,String], parse: String): Unit = {
    try{
      val finalUrl = getUrl(strUrl, parameters)
      // println("finalURL:"+finalUrl)
      val req = url(finalUrl)
      val response = Http(req OK as.String)
      response onComplete {
        case Success(content) =>
         //  println("get Success content:"+content)
        case Failure(t) =>
          // println("get Success content:"+t.getMessage)
      }
    }catch {
      case e:Exception =>{
        println("get Exception")
      }
    }
  }

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
