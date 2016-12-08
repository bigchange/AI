package com.bigchange.config

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/12/8.
  */
class TestDom4jParser  extends FlatSpec with Matchers {

  "getAllParameter" should "work" in  {

    val parser = Dom4jParser.apply("src/test/resources/message.xml")

    val t1 = System.currentTimeMillis()

    val parameters = parser.getAllParameter

    val t2 = System.currentTimeMillis()

    println(t2 - t1)

    parameters.size should be (6)

  }

}
