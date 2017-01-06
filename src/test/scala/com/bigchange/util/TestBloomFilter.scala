package com.bigchange.util

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by C.J.YOU on 2017/1/6.
  */
class TestBloomFilter extends FlatSpec with Matchers {

  "BloomFilter"  should "work" in {

    val bf = BloomFilter.apply(24)

    val data = Source.fromFile("src/test/resources/index_url_20170104").getLines().map(_.split("\t")(1))

    data.foreach(bf.add)

    bf.contain("smzdm.com") should be (true)

    bf.contain("kunyan.com") should be (false)

  }

}
