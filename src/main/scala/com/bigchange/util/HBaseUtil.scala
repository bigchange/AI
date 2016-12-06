package com.bigchange.util

import com.bigchange.log.CLogger
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by C.J.YOU on 2016/3/21.
  */
object HBaseUtil {

  private val hBaseConfiguration = getConfiguration

  private lazy val connection = getConnection

  private case class RowData(rowKey: String, dataMap:mutable.HashMap[String, String])

  // 自定义外部可访问接口：存储对应格式数据到hbase即可（部分对象 特征 可根据实际需求 再进行一层抽象）

  private def getConfiguration = {

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
  /**
    * 获取hbase的连接器
    * @return connection
    */
  private def getConnection: Connection = {

    CLogger.warn("create connection")

    val connection = ConnectionFactory.createConnection(hBaseConfiguration)

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
  private def  createHbaseTable(tableName: TableName,columnFamilys:List[String],connection: Connection): Unit ={
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
    * @param row rowKey
    * @param table tableName
    * @return Boolean
    */
  private def existRowKey(row:String,table:Table): Boolean ={

    val get = new Get(row.getBytes())
    val result = table.get(get)

    if (result.isEmpty) {
      CLogger.warn("hbase table don't have this data,execute insert")
      return false
    }

    true

  }

  /**
    * 数据格式转换
    * @param qualifiers  对应的列名集合
    * @param string 写入的数据内容
    * @return 自定义数据格式
    */
  private[this] def formatToRowTelecomData(qualifiers:Array[String], string:String)  = {

    val dataArray = string.split("\t")
    val dataMap = new mutable.HashMap[String, String]()

    for (index <- qualifiers.indices) {
      dataMap.put(qualifiers(index), dataArray(index))
    }
    RowData(rowKey = TimeUtil.getTimeStamp.toString, dataMap)

  }

  /**
    * 数据写入table中
    * @param tableName 表名
    * @param family  列族
    * @param qualifiers RowTelecomData 格式数据
    * @param originData  originData
    * @param dataFormatFunction dataFormatFunction  数据格式化函数
    * @return Boolean
    */
  private def put(tableName:String, family: String, qualifiers:Array[String], originData:String, dataFormatFunction: (Array[String], String) => RowData): Boolean = {

    val data = dataFormatFunction(qualifiers, originData)
    val hbaseTableName = TableName.valueOf(tableName)

    if (!connection.getAdmin.tableExists(hbaseTableName))
      createHbaseTable(hbaseTableName, List("info"), connection)

    val table = connection.getTable(hbaseTableName)

    // write data bytes
    data.dataMap.foreach { case(qualifier, value) =>
      if (existRowKey(data.rowKey, table))  return false
      val put = new Put(Bytes.toBytes(data.rowKey))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier.toString), Bytes.toBytes(value.toString))
      table.put(put)
    }

    true

  }

  /**
    * 通过rowKey 获取数据
    * @param rowKey  键
    * @param tableName 表名
    * @param family 列族
    * @param qualifiers 列名集合
    * @return Result of get
    */
  def get(rowKey: String, tableName:String, family: String, qualifiers:Array[String]) = {

    val htable = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))

    for (qualifier <- qualifiers) {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))
    }

    htable.get(get)

  }

  /**
    * 通过rdd 形式批量获取hbase数据
    * @param sc sparkContext
    * @param property 获取数据的属性：tableName, family, qualifiers , ...
    * @param scan  scan 定义
    * @return  RDD[Result]
    */
  def getHbaseDataThroughNewAPI(sc: SparkContext, property: Map[String, String], scan: Scan) = {

    val proto:ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hBaseConfiguration.set(TableInputFormat.SCAN, scanToString)

    property.foreach { case(key, value) =>
      hBaseConfiguration.set(key, value)
    }

    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConfiguration, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)

    hbaseRDD

  }

}
