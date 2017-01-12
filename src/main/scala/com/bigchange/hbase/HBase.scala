package com.bigchange.hbase

import com.bigchange.util.HBaseUtil._
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table}

/**
  * Created by C.J.YOU on 2017/1/12.
  */
class HBase {

  // 自定义访问hbase的接口（属于HBASE 的一些属性）

  def getConfiguration(property: Map[String, String]) = {

    val hbaseConf = HBaseConfiguration.create

    property.foreach{ case(key, value) =>

      hbaseConf.set(key, value)

    }

    hbaseConf

  }

  /**
    * 获取hbase的连接器
    * @return connection
    */
  def getConnection: Connection = {

    warn("create connection")

    val connection = ConnectionFactory.createConnection(getConfiguration)

    sys.addShutdownHook {
      connection.close ()
    }
    connection
  }

  /**
    * 创建hbase表
    * @param tableName 表名
    * @param columnFamilys 列族的声明
    * @param connection 连接器
    */
  def createHbaseTable(tableName: TableName,columnFamilys:List[String],connection: Connection): Unit ={

    // connection.getAdmin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily).setMaxVersions(3)))

    val admin = connection.getAdmin
    val htd = new HTableDescriptor(tableName)

    for (family <- columnFamilys) {

      val hcd = new HColumnDescriptor(family)
      htd.addFamily(hcd.setMaxVersions(3))

    }

    admin.createTable(htd)
    admin.close()

  }

  /**
    * 判断 row key 是否存在
    *
    * @param row rowKey
    * @param table tableName
    * @return Boolean
    */
  def existRowKey(row:String,table:Table): Boolean ={

    val get = new Get(row.getBytes())
    val result = table.get(get)

    if (result.isEmpty) {
      warn("hbase table don't have this data,execute insert")
      return false
    }

    true

  }


  def getConfiguration = {

    // 本地测试 需配置的选项， 在集群上通过对应配置文件路径自动获得
    val hbaseConf = HBaseConfiguration.create
    hbaseConf.set("fs.defaultFS", "hdfs://ns1"); // nameservices的路径
    hbaseConf.set("dfs.nameservices", "ns1");  //
    hbaseConf.set("dfs.ha.namenodes.ns1", "nn1,nn2"); //namenode的路径
    hbaseConf.set("dfs.namenode.rpc-address.ns1.nn1", "server3:9000"); // namenode 通信地址
    hbaseConf.set("dfs.namenode.rpc-address.ns1.nn2", "server4:9000"); // namenode 通信地址
    // 设置namenode自动切换的实现类
    hbaseConf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    hbaseConf.set("hbase.rootdir", "hdfs://ns1/hbase")
    hbaseConf.set("hbase.zookeeper.quorum", "server0,server1,server2")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    hbaseConf

  }

}
