package scala.util

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import scala.log.SUELogger
import scala.telecom.classification.UrlType

/**
  * Created by C.J.YOU on 2016/3/21.
  */
object HBaseUtil {

  private lazy val connection = getConnection
  private case class RowTelecomData(row:String,ts:String,ad:String,ua:String,url:String,ref:String,cookie:String)

  /**
    * 获取hbase的连接器
    * @return connection
    */
  private def getConnection: Connection ={

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
    SUELogger.warn("create connection")

    val connection = ConnectionFactory.createConnection(hbaseConf)
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
    * row key 是否存在
    */
  private def existRowKey(row:String,table:Table): Boolean ={

    val get = new Get(row.getBytes())
    val result = table.get(get)
    if (result.isEmpty) {
      SUELogger.warn("hbase table don't have this data,execute insert")
      return false
    }
    true
  }

  /**
    * 存入数据到对应的table中
    */
  private def put(tableName:String,data:RowTelecomData): Boolean ={

    val hbaseTableName = TableName.valueOf(tableName)
    if (!connection.getAdmin.tableExists(hbaseTableName))
      createHbaseTable(hbaseTableName, List("info"), connection)
    val table = connection.getTable(hbaseTableName)
    if (existRowKey(data.row, table)) {
       return  false
    }

    val put = new Put(Bytes.toBytes(data.row))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ts"), Bytes.toBytes(data.ts))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ad"), Bytes.toBytes(data.ad))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ua"), Bytes.toBytes(data.ua))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(data.url))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ref"), Bytes.toBytes(data.ref))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cookie"), Bytes.toBytes(data.cookie))
    table.put(put)
    true
  }

  private def formatData(string:String) : RowTelecomData ={
     val arr = string.split("\t")
     RowTelecomData(TimeUtil.getTimeStamp+"_"+ arr(1),arr(0),arr(1),arr(2),arr(3),arr(4),arr(5))
  }

  def saveData(arr:Array[String]): Unit ={
    for(item <- arr){
      val urlType = UrlType.apply(item.split("\t")(3))
      if(urlType.urlType.nonEmpty){
        put("Raw_"+urlType.urlType,formatData(item))
      }
    }
  }

}
