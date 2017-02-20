package com.bigchange.config

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2017/2/17.
  */
class TestXMLConfig  extends FlatSpec with Matchers {

  "getAllParameter" should "work" in  {

    val xmlConfig = Dom4jParser.apply("src/test/resources/config.xml")

    val t1 = System.currentTimeMillis()

    val parameters = xmlConfig.getAllParameter

    val t2 = System.currentTimeMillis()

    println("timeUnit:" + (t2 - t1))

    parameters.foreach(println)

    parameters.size should be (15)

  }

}
