package com.bigchange.config

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/12/9.
  */
 trait  Parameter {

  // spark.appName
  def getParameterByTagName(tagName: String):String

  def getAllParameter: mutable.HashMap[String, String]

}
