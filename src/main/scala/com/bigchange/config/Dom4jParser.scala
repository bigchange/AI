package com.bigchange.config

import java.io.File

import com.bigchange.log.CLogger
import org.dom4j.io.SAXReader
import org.dom4j.{Document, Element}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by C.J.YOU on 2016/12/8.
  */
class Dom4jParser (xmlFilePath:String) extends Parameter with CLogger  {

  private val parser = dom4jParser

  private var parameters = new mutable.HashMap[String, String]

  def dom4jParser = {

    val file = new File(xmlFilePath)
    val saxReader = new SAXReader()

    val doc = Try(saxReader.read(file))

    // note : scala de try operation is pattern
    val result = doc match  {
      case Success(r) => r
      case Failure(exception) => println("getDocumentExcetion:" +exception.getMessage)
    }

    result.asInstanceOf[Document]

  }

  override def toString = this.xmlFilePath

  // 遍历xml文件
  private  def elements(element: Element):  (String, String)= {

    val iterator = element.elementIterator()

    if(!iterator.hasNext) {
      return (element.getName, element.getTextTrim)
    }

    while(iterator.hasNext) {

      val subElement:Element = iterator.next().asInstanceOf[Element]

      val parent = element.getName

      val res = elements(subElement)

      if(res._1 != "")
        parameters.+=(parent + "." + res._1 -> res._2)

    }

    ("", "")

  }

  override def getAllParameter = {

    val root = parser.getRootElement
    if (parameters.isEmpty)
      elements(root)

    parameters

  }

  override def getParameterByTagName(tagName: String) = {

    if (parameters.isEmpty)
      getAllParameter

    val value = Try(parameters(tagName))

    val res = value match {
      case Success(v) => v
      case Failure(ex) => errorLog(msg = "tagName not exist!!")
    }

    res.asInstanceOf[String]

  }

}

object Dom4jParser {

  private var parser: Dom4jParser = _

  def apply(xmlFilePath: String): Dom4jParser = {

    if(parser == null)
      parser = new Dom4jParser(xmlFilePath)

    parser

  }

  def getInstance = parser

}
