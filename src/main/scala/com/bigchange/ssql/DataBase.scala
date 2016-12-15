package com.bigchange.ssql

import java.sql._

import com.bigchange.config.XMLConfig
import com.bigchange.log.CLogger
import com.jolbox.bonecp.{BoneCP, BoneCPConfig}

import scala.util.Try


// 创建 数据库 对应表的操作对象 进行进一步封装
case class Table(table:String)

/**
  * Created by C.J.YOU on 2016/3/22.
  * MYSQL 执行操作对象
  */
class MysqlHandler(connection: Connection) extends Serializable with  CLogger {

  private var conn = connection

  private  var statement: Statement = null

  def close() = {

    statement.close()
    conn.close()

  }

  def getConnection = conn

  private def getStatement  =  {
     if(statement == null)
       statement = conn.createStatement()
  }

  def this() = this(null)

  /**
  * 另一种初始化方法
  * @param url mysql配置的url地址
  * @param xml 全局XML句柄
  * @author youchaojiang
    */
  def this(url: String, xml: XMLConfig) = {

    this

    try {

      val parameter = (xml.getElem("MYSQL", "user"), xml.getElem("MYSQL", "password"))
      // 这个方法可以不必显示调用，判断标准为jar包的META-INF/services/目录的java.sql.Driver文件里是否包含
      // com.mysql.jdbc.Driver这行，在DriverManager被加载时的静态块中会遍历这个文件里的内容进行主动加载
      Class.forName(xml.getElem("MYSQL", "driver"))

      conn = DriverManager.getConnection(if(url.isEmpty) xml.getElem("MYSQL", "url") else url , parameter._1, parameter._2)

      getStatement

    } catch {

      case e: SQLException =>
        errorLog(logFileInfo, e.getMessage + "[SQLException]")
        System.exit(-1)

      case e: ClassNotFoundException =>
        errorLog(logFileInfo, e.getMessage + "[ClassNotFoundException]")
        System.exit(-1)

      case e: Exception =>
        errorLog(logFileInfo, e.getMessage)
        System.exit(-1)
    }
  }

  def addCommand(sql: String): Try[Unit] = {

    val ret = Try(statement.addBatch(sql))

    ret
  }

  // 执行sql 语句返回结果
  def executeQuery(sql: String): Try[ResultSet] = {

    Try(statement.executeQuery(sql))

  }

  /**
    * 执行插入操作
    * @param  sql sql语句
    *  execInsertInto("sql") recover {
          case e: Exception => warnLog(logFileInfo, e.getMessage + "[delete add failure]")
       }
    */
  def execInsertInto(sql: String): Try[Int] = {

    val ret = Try({

      val stmt = connection.createStatement
      val count = stmt.executeUpdate(sql)
      stmt.close()
      count
    })

    ret
  }
  execInsertInto(
    "sql"
  ) recover {
    case e: Exception => warnLog(logFileInfo, e.getMessage + "[delete add failure]")
  }

  /**
    * 执行更新操作
    * @param  sql sql语句
    *
    */
  def execUpdate(sql: String): Try[Int] = {

    val ret = Try({

      val stmt = connection.createStatement
      val count = stmt.executeUpdate(sql)
      stmt.close()
      count
    })

    ret
  }

}

// MysqlHandle伴生对象
object MysqlHandler {

  def apply(connect: Connection):  MysqlHandler = {
    new MysqlHandler(connect)
  }

  def apply(
             url: String,
             xml: XMLConfig
           ): MysqlHandler = {
    new MysqlHandler(url, xml)
  }

}

/**
  * 数据库连接池（集群模式需广播连接池）
  * @param xmlHandle  配置
  * @param isTestOrNot 是否使用测试数据库
  */
class MysqlPool private(val xmlHandle: XMLConfig, val isTestOrNot: Boolean = true) extends Serializable {

    lazy val config = createConfig

    lazy val connPool = new BoneCP(config)

    /** 初始化数据库连接池 */
    def createConfig: BoneCPConfig = {

      val initConfig = new BoneCPConfig

      if(isTestOrNot) {

        initConfig.setJdbcUrl(xmlHandle.getElem("mySql", "urltest"))
        initConfig.setUsername(xmlHandle.getElem("mySql", "usertest"))
        initConfig.setPassword(xmlHandle.getElem("mySql", "passwordtest"))

      } else {
        initConfig.setJdbcUrl(xmlHandle.getElem("mySql", "urlstock"))
        initConfig.setUsername(xmlHandle.getElem("mySql", "userstock"))
        initConfig.setPassword(xmlHandle.getElem("mySql", "passwordstock"))
      }

      initConfig.setMinConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mySql", "minconn")))
      initConfig.setMaxConnectionsPerPartition(Integer.parseInt(xmlHandle.getElem("mySql", "maxconn")))
      initConfig.setPartitionCount(Integer.parseInt(xmlHandle.getElem("mySql", "partition")))
      initConfig.setConnectionTimeoutInMs(Integer.parseInt(xmlHandle.getElem("mySql", "timeout")))
      initConfig.setConnectionTestStatement("select 1")
      initConfig.setIdleConnectionTestPeriodInMinutes(Integer.parseInt(xmlHandle.getElem("mySql", "connecttest")))

      initConfig

    }

    def setConfig(mix: Int, max: Int, testPeriod: Long) = {
      config.setPartitionCount(1)
      config.setMinConnectionsPerPartition(mix)
      config.setMaxConnectionsPerPartition(max)
      config.setIdleConnectionTestPeriodInMinutes(3)
      config.setIdleMaxAgeInMinutes(3)
    }

    // get connection
    def getConnect: Option[Connection] = {

      var connect: Option[Connection] = null

      try {
        connect = Some(connPool.getConnection)
      } catch {

        case e: Exception =>

          if(connect != null) {
            connect.get.close()
          }
          connect = None
      }

      connect
    }

    def close() = connPool.shutdown()

  }

  /**
    * Created by wukun on 2016/5/18
    * MysqlPool伴生对象
    */
  object MysqlPool extends Serializable {

    def apply(xmlHandle: XMLConfig, isTestOrNot: Boolean = true) = new MysqlPool(xmlHandle, isTestOrNot)


  }
