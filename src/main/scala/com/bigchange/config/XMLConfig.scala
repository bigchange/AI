package com.bigchange.config

import scala.xml.XML

/**
  * Created by C.J.YOU on 2016/8/26.
  * 配置文件的加载
  * same to SXMLConfig
  */

class XMLConfig(xmlFilePath:String) extends  Dom4jParser(xmlFilePath:String) with  Parameter {

  private val xmlConfig = loadXml()

  def loadXml() = XML.loadFile(xmlFilePath)

  def getElem(elemName:String) = (xmlConfig \ elemName).text

  def getElem(firstName:String, secondName:String) = (xmlConfig \ firstName \ secondName).text

  def  allParameter = xmlConfig

  override def toString = this.xmlFilePath


}


// 伴生对象
object XMLConfig {

  private  var xmlHandle: XMLConfig =  null

  /**
    * 获取全局唯一的操作句柄实例
    * @param xmlFilePath 路径
    * @author youchaojiang
    */
  def apply(xmlFilePath: String)  = {

    if (xmlHandle == null)
      xmlHandle = new XMLConfig(xmlFilePath)

    xmlHandle.dom4jParser
    xmlHandle.getAllParameter

    xmlHandle

  }

  def getInstance = xmlHandle

  // 如何动态增加 外部参数变量
  def getAllXmlParameter = xmlHandle.allParameter



}
