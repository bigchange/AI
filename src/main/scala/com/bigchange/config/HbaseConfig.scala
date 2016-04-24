package com.bigchange.config

/**
  * Created by C.J.YOU on 2016/1/17.
  * Hbase的配置文件
  */
object HbaseConfig {

  /*
  val HBASE_URL = "hdfs://server:9000"
  val HBASE_ROOT_DIR = "hdfs://server:9000/hbase"
  val HBASE_ZOOKEEPER_QUORUM = "server"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"
  */
  val HBASE_URL = "hdfs://ns1"
  val HBASE_ROOT_DIR = "hdfs://ns1/hbase"
  val HBASE_ZOOKEEPER_QUORUM = "server0,server1,server2"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

}
