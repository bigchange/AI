package com.bigchange.config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  */

class SXMLConfig(xmlFilePath:String) {


  private val xmlConfig = loadXml


  def loadXml = XML.loadFile(xmlFilePath)

  def getElem(elemName:String) = (xmlConfig \ elemName).text

  def getElem(firstName:String, secondName:String) = (xmlConfig \ firstName \ secondName).text

  override def toString = this.xmlFilePath

}


// 伴生对象
object SXMLConfig {

  private  var  xmlHandler: SXMLConfig =  null

  def apply(xmlFilePath: String) = {

    if( xmlHandler == null)
       xmlHandler = new SXMLConfig(xmlFilePath)

    xmlHandler

  }

  def getInstance = xmlHandler

}
