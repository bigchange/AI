package com.bigchange.hbase

import com.bigchange.util.HBaseUtil._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.SparkContext

/**
  * Created by C.J.YOU on 2017/1/12.
  */
class HBase {

  // 自定义访问hbase的接口（属于HBASE 的一些属性）

  private var hBaseConfiguration: Configuration = null

  /**
    * 通过rdd 形式批量获取hbase数据
    * @param sc sparkContext
    * @param property 获取数据的属性：tableName, family, qualifiers , ...
    * @param scan  scan 定义
    * @return  RDD[Result]
    */
  def getHBaseDataThroughNewAPI(sc: SparkContext, property: Map[String, String], scan: Scan) = {

    val proto:ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)

    if(hBaseConfiguration == null)
      throw new Exception(s"please init configuration first")

    hBaseConfiguration.set(TableInputFormat.SCAN, scanToString)

    property.foreach { case(key, value) =>
      hBaseConfiguration.set(key, value)
    }

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2 )

    hbaseRDD

  }

  /**
    * 配置HBase的参数信心
    * @param property 参数
    * @return Configuration
    */
  def setConfiguration(property: Map[String, String]) = {

    hBaseConfiguration = HBaseConfiguration.create

    property.foreach{ case(key, value) =>

      hBaseConfiguration.set(key, value)

    }

    hBaseConfiguration

  }

  /**
    * 获取HBase的连接器
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
    * @param columnFamilies 列族的声明
    * @param connection 连接器
    */
  def createHBaseTable(tableName: TableName, columnFamilies:List[String], connection: Connection): Table = {

    // connection.getAdmin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily).setMaxVersions(3)))

    val admin = connection.getAdmin
    val htd = new HTableDescriptor(tableName)

    for (family <- columnFamilies) {

      val hcd = new HColumnDescriptor(family)
      htd.addFamily(hcd.setMaxVersions(3))

    }

    admin.createTable(htd)
    admin.close()

    val table = connection.getTable(tableName)

    table

  }


  /**
    * 判断 row key 是否存在
    * @param row rowKey
    * @param table tableName
    * @return Boolean
    */
  def existRowKey(row:String, table: Table): Boolean ={

    val get = new Get(row.getBytes())
    val result = table.get(get)

    if (result.isEmpty) {
      warn("hbase table don't have this data,execute insert")
      return false
    }

    true

  }

  def getConfiguration = if(hBaseConfiguration == null) {
      warn("hbase setDefaultConfiguration....")
      setDefaultConfiguration
    } else  hBaseConfiguration

  def setDefaultConfiguration = {

    hBaseConfiguration = HBaseConfiguration.create

    // 本地测试 需配置的选项， 在集群上通过对应配置文件路径自动获得
    hBaseConfiguration.set("fs.defaultFS", "hdfs://ns1"); // nameservices的路径
    hBaseConfiguration.set("dfs.nameservices", "ns1");  //
    hBaseConfiguration.set("dfs.ha.namenodes.ns1", "nn1,nn2"); //namenode的路径
    hBaseConfiguration.set("dfs.namenode.rpc-address.ns1.nn1", "server3:9000"); // namenode 通信地址
    hBaseConfiguration.set("dfs.namenode.rpc-address.ns1.nn2", "server4:9000"); // namenode 通信地址
    // 设置namenode自动切换的实现类
    hBaseConfiguration.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    hBaseConfiguration.set("hbase.rootdir", "hdfs://ns1/hbase")
    hBaseConfiguration.set("hbase.zookeeper.quorum", "server0,server1,server2")
    hBaseConfiguration.set("hbase.zookeeper.property.clientPort", "2181")

    hBaseConfiguration

  }

}
