package com.bigchange.config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class XMLConfig(xmlFilePath:String) {


  val XML_CONFIG  = XML.loadFile(xmlFilePath)

  val IP = ( XML_CONFIG  \ "FTP" \ "IP").text

  val BACK_IP = ( XML_CONFIG  \ "FTP" \ "BACKIP").text

  val USER_NAME = ( XML_CONFIG  \ "FTP" \ "USER").text

  val PASSWORD = ( XML_CONFIG  \ "FTP" \ "PASSWORD" ).text

  val REMOTE_DIR = (XML_CONFIG  \ "FTP" \ "ROOTDIR").text

  val FILE_PREFIX_NAME = (XML_CONFIG  \ "FTP" \ "PREFFIX").text

  val FILE_SUFFIX_NAME = (XML_CONFIG  \ "FTP" \ "SUFFIX" ).text

  val LOG_DIR = (XML_CONFIG  \ "FILE" \ "LOG" ).text

  var DATA_DIR = (XML_CONFIG  \ "FILE" \ "DATA").text

  var PROGRESS_DIR =( XML_CONFIG  \ "FILE" \ "PROCESS" ).text

  val LOG_CONFIG = (XML_CONFIG  \ "LOGGER" \ "CONF").text

  val RECEIVER = (XML_CONFIG \ "Message" \ "receiver").text

  val KEY = (XML_CONFIG \ "Message" \ "key").text

  val MESSAGE_CONTEXT = (XML_CONFIG \ "Message" \ "context").text


}


// 伴生对象
object XMLConfig {

  var ftpConfig: XMLConfig =  null

  def apply(xmlFilePath: String)  = {

    if (ftpConfig == null) ftpConfig = new XMLConfig(xmlFilePath) else ftpConfig

  }


}
