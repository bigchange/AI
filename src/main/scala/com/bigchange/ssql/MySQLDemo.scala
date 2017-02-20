package com.bigchange.ssql

import com.bigchange.config.XMLConfig

/**
  * Created by C.J.YOU on 2017/2/17.
  * MySql 操作实例
  */
object MySQLDemo {

  def main(args: Array[String]) {

    // val Array(xmlPath) = args
    val  xmlPath = "src/test/resources/config.xml"
    val pool = MysqlPool.apply(XMLConfig.apply(xmlPath), isTestOrNot = false)
    val connection = pool.getConnect
    val handler = MysqlHandler.apply(connection.get)
    val res = handler.executeQuery("select count(*) from events").get

    while(res.next()) {
        println(res.getString(1))
    }

    pool.close()

  }

}
