package com.bigchange.config

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/12/8.
  * spark log output test
  */
class TestDom4jParser  extends FlatSpec with Matchers {

  "getAllParameter" should "work" in  {

    val parser = Dom4jParser.apply("src/test/resources/config.xml")

    val t1 = System.currentTimeMillis()

    val parameters = parser.getAllParameter

    val t2 = System.currentTimeMillis()

    println("timeUnit:" + (t2 - t1))

    parameters.foreach(println)

    parameters.size should be (15)

  }

}
